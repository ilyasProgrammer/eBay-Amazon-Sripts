"""
This takes care of repricing eBay listings that are of brand Vintage2Modern (ie listings with V2F in custom_label)
Compete with listings with seller marked classic2current
"""

import argparse
import logging
import opsystConnection
import os
import simplejson
import sys

from datetime import datetime, timedelta
import ebaysdk
from ebaysdk.exception import ConnectionError
from ebaysdk.shopping import Connection as Shopping
from ebaysdk.trading import Connection as Trading

parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-offset', action="store", dest="offset")
parser.add_argument('-limit', action="store", dest="limit")
parser.add_argument('-test-only', action="store", dest="test_only")
args = vars(parser.parse_args())
offset = int(args['offset']) if args['offset'] else 0
limit = int(args['limit']) if args['limit'] else 100
test_only = True if 'test_only' in args and args['test_only'] else False

thisdir = os.path.abspath(os.path.dirname( __file__ ))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', filename='/mnt/vol/log/cron/c2c_repricer.log', datefmt='%Y-%m-%d %H:%M:%S')
# logging.basicConfig(level=logging.INFO)

odoo_url = config.get('odoo_url')
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
stores = config.get('stores').split(',')

def main():

    Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
    credentials = Connection.get_store_credentials(['visionary'])['visionary']
    ebay_shopping_api = Shopping(config_file=None, appid=credentials['application_key'], siteid='100')
    ebay_trading_api = Trading(domain='api.ebay.com',
       config_file=None,
       appid=credentials['application_key'],
       devid=credentials['developer_key'],
       certid=credentials['certificate_key'],
       token=credentials['auth_token'],
       siteid='100'
    )

    # Process at most 100 listings at a time ONLY

    offset_for_query = offset
    limit_for_query = min(limit, 100)

    while offset_for_query < (offset + limit):
        listings_rows = Connection.odoo_execute("""
            SELECT L.id as listing_id, PT.id as product_tmpl_id, PT.part_number, L.name, L.current_price,
            REP.type, REP.percent, REP.amount
            FROM product_listing L
            LEFT JOIN product_template PT on PT.id = L.product_tmpl_id
            LEFT JOIN repricer_scheme REP on REP.id = L.repricer_scheme_id
            WHERE L.custom_label LIKE 'V2F-%%' ORDER BY L.id LIMIT %s OFFSET %s
        """, [limit_for_query, offset_for_query])

        product_tmpl_id_list = [r['product_tmpl_id'] for r in listings_rows]

        if product_tmpl_id_list:

            listings_res = dict((r['product_tmpl_id'], {
                'item_id': r['name'],
                'current_price': r['current_price'],
                'listing_id': r['listing_id'],
                'part_number': r['part_number'],
                'type': r['type'] or 'percent',
                'percent': r['percent'] or 1,
                'amount': r['amount'] or 1
            }) for r in listings_rows)

            competitor_rows = Connection.odoo_execute("""
                SELECT PT.id as product_tmpl_id, COMP.id as competitor_id, COMP.item_id FROM repricer_competitor COMP
                LEFT JOIN product_template PT on PT.id = COMP.product_tmpl_id AND COMP.seller = 'classic2currentfabrication'
                WHERE PT.id IN %s
            """, [product_tmpl_id_list])

            competitor_res = {}
            for competitor_row in competitor_rows:
                if competitor_row['product_tmpl_id'] not in competitor_res:
                    competitor_res[competitor_row['product_tmpl_id']] =  [
                        {
                            'item_id': competitor_row['item_id'],
                            'competitor_id': competitor_row['competitor_id']
                        }
                    ]
                else:
                    competitor_res[competitor_row['product_tmpl_id']].append(
                        {
                            'item_id': competitor_row['item_id'],
                            'competitor_id': competitor_row['competitor_id']
                        }
                    )

            uses_ebay_repricer = []
            listings_with_current_price_updates = []
            current_price_cases = ''

            for product_tmpl_id in competitor_res:
                competitor_price = 0
                try:
                    item_ids = [r['item_id'] for r in competitor_res[product_tmpl_id]]
                    result = ebay_shopping_api.execute('GetMultipleItems', {'ItemID': item_ids, 'IncludeSelector': 'Details'}).dict()
                    if 'Item' in result:
                        items = result['Item']
                        if not isinstance(items, list):
                            items = [items]
                        if any(int(item['Quantity']) > 0 for item in items):
                            competitor_price = min(float(item['ConvertedCurrentPrice']['value']) for item in items if int(item['Quantity']) > 0)
                except:
                    logging.error('Something went wrong with the API call')

                listing = listings_res[product_tmpl_id]
                uses_ebay_repricer.append(listing['listing_id'])

                min_cost = Connection.get_non_kit_min_cost([listing['part_number']])[listing['part_number']]
                new_price = 1.3 * min_cost
                if competitor_price > 0:
                    new_price = Connection.ebay_get_new_price(min_cost, competitor_price, listing['type'], listing['percent'], listing['amount'])

                formatted_current_price = '{0:.2f}'.format(listing['current_price'])
                formatted_new_price = '{0:.2f}'.format(new_price)
                
                if formatted_new_price != formatted_current_price and new_price > 0:
                    item_dict = {'ItemID': listing['item_id'], 'StartPrice': formatted_new_price }
                    try:
                        ebay_trading_api.execute('ReviseItem', {'Item': item_dict})
                        current_price_cases += "WHEN id=%s THEN %s" % (listing['listing_id'], round(new_price, 2))
                        listings_with_current_price_updates.append(listing['listing_id'])
                        logging.info('Revised price item %s from %s to %s against competitor with price %s'
                            % (listing['item_id'], formatted_current_price, formatted_new_price, competitor_price))
                    except Exception as e:
                        logging.error('Revising item %s failed: %s', listing['item_id'], e)
                    Connection.odoo_execute("""
                        UPDATE product_listing 
                        SET current_price = %s, price_history = TO_CHAR(NOW() AT TIME ZONE 'EST', 'YYYY-MM-DD HH24:MI:SS') || '  ' || ROUND(%s, 2) ||E'\n' || coalesce(price_history, '') 
                        WHERE id = %s
                    """ % (new_price, new_price, listing['listing_id']), commit=True)

            if uses_ebay_repricer:
                Connection.odoo_execute("""
                    UPDATE product_listing SET ebay_reprice_against_competitors = true WHERE id IN %s
                """, [uses_ebay_repricer], commit=True)
            if listings_with_current_price_updates:
                listing_ids_for_query = Connection.parse_parameters_for_sql([listings_with_current_price_updates])[0]
                Connection.odoo_execute("""
                    UPDATE product_listing SET current_price = (CASE %s END) WHERE id IN %s
                """ %(current_price_cases, listing_ids_for_query), commit=True)
        if len(listings_rows) < 100:
            break
        offset_for_query += 100

if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-

"""
Update ebay competitor prices
Competitor listings will be checked for price changes more frequently than others
depending on sequence
Ex. Those with sequence 1 to 100 will be checked twice a day while those with sequence
101-1000 will be once a day
"""

import logging
import argparse
import os
import simplejson
from datetime import datetime
from ebaysdk.shopping import Connection as Shopping
from ebaysdk.trading import Connection as Trading
import opsystConnection
import getpass
import sys
import slack
import socket


this_module_name = os.path.basename(sys.modules['__main__'].__file__)
logging.captureWarnings(True)
logging.getLogger('requests').setLevel(logging.ERROR)
current_user = getpass.getuser()
current_host = socket.gethostname()
parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-from', action="store", dest="from")
parser.add_argument('-to', action="store", dest="to")
parser.add_argument('-test-only', action="store", dest="test_only")
args = vars(parser.parse_args())
if args['test_only'] in ('0', 'False'):
    test_only = False
if current_host == 'pc':
    test_only = False
    from_sequence = 1
    to_sequence = 1
else:
    from_sequence = int(args['from']) or 0
    to_sequence = int(args['to']) or 100
    test_only = bool(args['test_only'])

thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')

LOG_FILE = '/mnt/vol/log/cron/ebay_competitor_prices_%s.log' % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)

Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials(['visionary'])['visionary']
ebay_shopping_api = Shopping(config_file=None, appid=credentials['application_key'], siteid='100')
ebay_trading_api = Trading(domain='api.ebay.com', config_file=None, appid=credentials['application_key'], devid=credentials['developer_key'],
                           certid=credentials['certificate_key'], token=credentials['auth_token'], siteid='100')


def main():
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    slack.notify_slack(this_module_name, "To process competitors with priority %s to %s" % (from_sequence, to_sequence))
    logging.info("To process competitors with priority %s to %s" % (from_sequence, to_sequence))
    # Sometimes item_id has spaces. Lets just remove it.
    remove_spaces = Connection.odoo_execute("""UPDATE repricer_competitor SET item_id = TRIM(item_id) where item_id like '% %'""", commit=True)
    comp_rows = Connection.odoo_execute("""
        SELECT C.id, C.item_id, C.price, C.quantity_sold FROM repricer_competitor C
        WHERE C.state = 'active' AND C.sequence >= %s AND C.sequence <= %s
        ORDER BY C.id
    """, [from_sequence, to_sequence])
    update_in_repricer = []
    comp_rows_len = len(comp_rows)
    batches = comp_rows_len / 20
    if comp_rows_len % 20 == 0:
        batches -= 1

    logging.info("To process %s competitor listings in %s batches" % (comp_rows_len, batches))
    batch = 0
    while batch <= batches:
        # logging.info("Processing batch: %s." % batch)
        batch_item_list = comp_rows[batch * 20: min(comp_rows_len, (batch + 1) * 20)]
        batch_item_ids = [r['item_id'].strip() for r in batch_item_list]
        batch_dict = {}
        for r in batch_item_list:
            batch_dict[r['item_id']] = {
                'price': "{0:.2f}".format(float(r['price']) if r['price'] else 0),
                'quantity_sold': r['quantity_sold'] or 0,
                'id': r['id']
            }

        result = {}
        try:
            result = ebay_shopping_api.execute('GetMultipleItems', {'ItemID': batch_item_ids, 'IncludeSelector': 'Details'}).dict()
        except Exception as e:
            batch += 1
            logging.error('Something went wrong with the API call. %s', e)
            logging.error('Batch: %s', batch_item_ids)
            for it in batch_item_ids:
                try:
                    rs = ebay_trading_api.execute('GetItem', {'ItemID': it, 'DetailLevel': 'ReturnSummary'})
                    if rs.reply.Ack != 'Success':
                        logging.error('Bad item id: %s', it)
                except Exception as ex:
                    logging.error(ex)
                    logging.error('Bad item id: %s', it)
            continue

        if 'Errors' in result:
            ended_competitors = result['Errors']['ErrorParameters']['Value'].split(',')
            Connection.odoo_execute("""
                UPDATE repricer_competitor SET state = 'inactive' WHERE item_id IN %s
            """, [ended_competitors], commit=True)

        if 'Item' in result:
            items = result['Item']
            if not isinstance(items, list):
                items = [items]

            history_values = ''
            price_cases = ''
            previous_price_cases = ''
            title_cases = ''
            quantity_cases = ''
            quantity_sold_cases = ''
            seller_cases = ''
            with_price_change_list = []
            item_ids_to_update_list = []

            for res in items:
                c_price = res['ConvertedCurrentPrice']['value']
                c_quantity = int(res['Quantity'])
                c_item_id = res['ItemID']
                c_title = unicode(res['Title']).encode('ascii', 'ignore').replace("'", "").replace("%", "%%")
                c_seller = res['Seller']['UserID']
                c_quantity_sold = int(res['QuantitySold'])

                if c_quantity and c_price != batch_dict[c_item_id]['price']:
                    price_cases += "WHEN item_id = '%s' THEN %s " % (c_item_id, float(c_price))
                    previous_price_cases += "WHEN item_id = '%s' THEN %s " % (c_item_id, float(batch_dict[c_item_id]['price']))
                    with_price_change_list.append(c_item_id)

                title_cases += "WHEN item_id = '%s' THEN '%s' " %(c_item_id, c_title)
                quantity_cases += "WHEN item_id = '%s' THEN %s " % (c_item_id, c_quantity)
                seller_cases += "WHEN item_id = '%s' THEN '%s' " % (c_item_id, c_seller)
                quantity_sold_cases += "WHEN item_id = '%s' THEN %s " % (c_item_id, c_quantity_sold)

                if c_quantity_sold > batch_dict[c_item_id]['quantity_sold']:
                    history_values += "(%s, %s, %s, now() at time zone 'UTC')," % (batch_dict[c_item_id]['id'], c_quantity_sold - batch_dict[c_item_id]['quantity_sold'], (c_price))

                item_ids_to_update_list.append(c_item_id)

            if price_cases:
                query = """UPDATE repricer_competitor
                           SET price = (CASE %s END),
                           previous_price = (CASE %s END),
                           price_write_date = now() at time zone 'UTC'""" % (price_cases, previous_price_cases)
                Connection.odoo_execute(query + "WHERE item_id IN %s", [with_price_change_list], commit=True)
                update_in_repricer += with_price_change_list
            query = """
                UPDATE repricer_competitor
                SET title = (CASE %s END),
                quantity = (CASE %s END),
                seller = (CASE %s END),
                quantity_sold = (CASE %s END)
            """ % (title_cases, quantity_cases, seller_cases, quantity_sold_cases)
            Connection.odoo_execute(query + "WHERE item_id IN %s", [item_ids_to_update_list], commit=True)

            if history_values:
                Connection.odoo_execute("""
                    INSERT INTO repricer_competitor_history (comp_id, qty_sold, price, create_date) VALUES
                    %s
                """ % history_values[:-1], commit=True)
        batch += 1
    # In the end Reprice listings on competitors price change
    # if with_price_change_list:
    #     v2_ebay_upload_stock_and_price_update.main(with_price_change_list)
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

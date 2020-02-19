"""
Update ebay competitor prices
Competitor listings will be checked for price changes more frequently than others
depending on sequence
Ex. Those with sequence 1 to 100 will be checked twice a day while those with sequence
101-1000 will be once a day
"""

import argparse
import logging
import os
import simplejson
from datetime import datetime
from ebaysdk.shopping import Connection as Shopping
import opsystConnection
import getpass
import sys
import slack

logging.captureWarnings(True)
logging.getLogger('requests').setLevel(logging.ERROR)
current_user = getpass.getuser()
this_module_name = os.path.basename(sys.modules['__main__'].__file__)
parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-test-only', action="store", dest="test_only")
args = vars(parser.parse_args())
test_only = bool(args['test_only'])
if args['test_only'] in ('0', 'False'):
    test_only = False

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

LOG_FILE = '/mnt/vol/log/cron/ebay_listings_%s.log' % datetime.today().strftime('%Y_%m')
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


def main():
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    listing_rows = Connection.odoo_execute("""
        SELECT LISTING.id, LISTING.name
        FROM product_listing LISTING
        LEFT JOIN sale_store STORE ON STORE.id = LISTING.store_id
        WHERE LISTING.state = 'active' AND STORE.site = 'ebay'
        ORDER BY LISTING.id
    """)

    listing_rows_len = len(listing_rows)
    batches = listing_rows_len / 20
    if listing_rows_len % 20 == 0:
        batches -= 1

    logging.info("To process %s listings in %s batches" % (listing_rows_len, batches))
    slack.notify_slack(this_module_name, "To process %s listings in %s batches" % (listing_rows_len, batches))
    batch = 0
    while batch <= batches:
        if batch % 100 == 0:
            logging.info("Processing batch %s." % batch)
        batch_item_list = listing_rows[batch * 20: min(listing_rows_len, (batch + 1) * 20)]
        batch_item_ids = [r['name'] for r in batch_item_list]
        result = {}
        try:
            result = ebay_shopping_api.execute('GetMultipleItems',
                                               {'ItemID': batch_item_ids, 'IncludeSelector': 'Details'}).dict()
        except:
            batch += 1
            logging.error('Something went wrong with the API call')
            continue

        if 'Errors' in result:
            ended_listings = result['Errors']['ErrorParameters']['Value'].split(',')
            Connection.odoo_execute("""
                UPDATE product_listing SET state = 'ended' WHERE name IN %s
            """, [ended_listings], commit=True)

        if 'Item' in result:
            items = result['Item']
            if not isinstance(items, list):
                items = [items]

            title_cases = ''
            quantity_sold_cases = ''
            item_ids_to_update_list = []

            for res in items:
                r_item_id = res['ItemID']
                r_title = unicode(res['Title']).encode('ascii', 'ignore').replace("'", "").replace('%', '%%')
                r_quantity_sold = int(res['QuantitySold'])

                title_cases += "WHEN name = '%s' THEN '%s' " %(r_item_id, r_title)
                quantity_sold_cases += "WHEN name = '%s' THEN %s " % (r_item_id, r_quantity_sold)
                item_ids_to_update_list.append(r_item_id)

            if item_ids_to_update_list:
                query = """
                    UPDATE product_listing
                    SET title = (CASE %s END),
                    qty_sold = (CASE %s END)
                """ % (title_cases, quantity_sold_cases)
                Connection.odoo_execute(query + "WHERE name IN %s", [item_ids_to_update_list], commit=True)
        batch += 1
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")


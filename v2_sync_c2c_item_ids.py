# -*- coding: utf-8 -*-

"""
This script syncs c2c item ids of c2c scraped by Usman
with c2c item ids present in Opsyst's repricer_competitor
"""

import argparse
import logging
import os
import simplejson
from ebaysdk.shopping import Connection as Shopping
from datetime import datetime
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

logs_path = config.get('logs_path')
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
ebay_app_id = config.get('ebay_app_id')
ebay_shopping_api = Shopping(config_file=None, appid=ebay_app_id, siteid='100')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')

LOG_FILE = os.path.join(logs_path + 'sync_c2c_item_ids_%s.log') % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)

Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
c2c_unmapped_product_res = Connection.odoo_execute("""SELECT id FROM product_template WHERE name = 'C2C Unmapped Competitor'""")


def add_new_competitors(items):
    values = ''
    for item in items:
        mpn = get_item_spec_val(item, 'Manufacturer Part Number')
        sku = Connection.autoplus_execute("""
             SELECT INV.PartNo AS partno
             FROM Inventory INV
             LEFT JOIN InventoryAlt ALT on ALT.InventoryID = INV.InventoryID
             LEFT JOIN Inventory INV2 on ALT.InventoryIDAlt = INV2.InventoryID
             WHERE INV.MfgID = 1 AND INV2.MfgID in (16,35,36,37,38,39) AND INV2.PartNo = '%s'
        """ % mpn)
        if sku:
            product = Connection.odoo_execute("""SELECT id FROM product_template WHERE name = '%s'""" % sku[0]['partno'])
            if product:
                logging.info('Found product by C2C: %s', product)
                product_tmpl_id = product[0]['id']
            else:
                logging.info('Product not found. MPN: %s', mpn)
                product_tmpl_id = c2c_unmapped_product_res[0]['id']
            values += "('%s', %s, '%s', 'classic2currentfabrication', 1000, 'active', 1, 1," \
                      "now() at time zone 'UTC', now() at time zone 'UTC')," % \
                      (item['ItemID'], product_tmpl_id, mpn)
    if values:
        logging.info('Inserting lines to odoo ...')
        Connection.odoo_execute("""
                            INSERT INTO repricer_competitor (item_id, product_tmpl_id, mfr_part_number, seller, sequence, state, create_uid, 
                            write_uid, create_date, write_date) 
                            VALUES %s
                        """ % values[:-1], commit=True)


def update_competitors(items):
    for item in items:
        mpn = get_item_spec_val(item, 'Manufacturer Part Number')
        sku = Connection.autoplus_execute("""
             SELECT INV.PartNo AS partno
             FROM Inventory INV
             LEFT JOIN InventoryAlt ALT on ALT.InventoryID = INV.InventoryID
             LEFT JOIN Inventory INV2 on ALT.InventoryIDAlt = INV2.InventoryID
             WHERE INV.MfgID = 1 AND INV2.MfgID in (16,35,36,37,38,39) AND INV2.PartNo = '%s'
        """ % mpn)
        if sku:
            product = Connection.odoo_execute("""SELECT id FROM product_template WHERE name = '%s'""" % sku[0]['partno'])
            if product:
                logging.info('Found product by C2C: %s', product)
                product_tmpl_id = product[0]['id']
            else:
                logging.info('Product not found. MPN: %s', mpn)
                product_tmpl_id = c2c_unmapped_product_res[0]['id']
            logging.info('Updating item %s in odoo ...', item['ItemID'])
            Connection.odoo_execute("""UPDATE repricer_competitor 
                                       SET product_tmpl_id = %s, mfr_part_number = '%s'
                                       WHERE seller = 'classic2currentfabrication' 
                                       AND item_id = '%s'""" % (product_tmpl_id, mpn, item['ItemID']), commit=True)


def sync_all_item_ids():
    logging.info('Syncing all c2c item ids...')
    ep_item_ids_res = Connection.ebayphantom_execute("""
        SELECT eBayItemID FROM ebayphantom.eBayItemIDs WHERE StoreID = 1
    """)
    ep_item_ids = [r['eBayItemID'] for r in ep_item_ids_res]

    ops_item_ids_res = Connection.odoo_execute("""
        SELECT item_id FROM repricer_competitor 
        WHERE seller = 'classic2currentfabrication'
    """)
    ops_item_ids = [r['item_id'] for r in ops_item_ids_res]
    logging.info('Computing new item ids...')
    new_item_ids = [r for r in ep_item_ids if r not in ops_item_ids]

    if new_item_ids:
        chunks = split_on_chunks(new_item_ids, 20)
        batch = 1
        batches = len(new_item_ids)/20
        for chunk in chunks:
            result = ebay_shopping_api.execute('GetMultipleItems', {'ItemID': chunk, 'IncludeSelector': 'ItemSpecifics'}).dict()
            if 'Item' in result:
                items = result['Item']
                if not isinstance(items, list):
                    items = [items]
                logging.info('Adding batch %s C2C competitors of %s...' % (batch, batches))
                add_new_competitors(items)
                batch += 1

    logging.info('Getting old item ids...')
    ops_old_item_ids_res = Connection.odoo_execute("""
        SELECT item_id, id FROM repricer_competitor 
        WHERE seller = 'classic2currentfabrication' and product_tmpl_id = %s""" % c2c_unmapped_product_res[0]['id'])
    if ops_old_item_ids_res:
        chunks = split_on_chunks(ops_old_item_ids_res, 20)
        batch = 1
        batches = len(ops_old_item_ids_res)/20
        for chunk in chunks:
            result = ebay_shopping_api.execute('GetMultipleItems', {'ItemID': [r['item_id'] for r in chunk], 'IncludeSelector': 'ItemSpecifics'}).dict()
            if 'Item' in result:
                items = result['Item']
                if not isinstance(items, list):
                    items = [items]
                logging.info('Updating batch %s C2C competitors of %s...' % (batch, batches))
                update_competitors(items)
                batch += 1


def get_item_spec_val(itm, spec_name):
    res = ''
    if itm.get('ItemSpecifics'):
        if type(itm['ItemSpecifics']['NameValueList']) == dict:
            if itm['ItemSpecifics']['NameValueList']['Name'] == spec_name:
                res = itm['ItemSpecifics']['NameValueList']['Value']
        elif type(itm['ItemSpecifics']['NameValueList']) == list:
            for spec in itm['ItemSpecifics']['NameValueList']:
                if spec['Name'] == spec_name:
                    res = spec['Value']
                    break
    return res


def split_on_chunks(lst, num):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(lst), num):
        yield lst[i:i + num]


def main():
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    sync_all_item_ids()
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")


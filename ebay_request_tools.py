# # -*- coding: utf-8 -*-


import random
import psycopg2
from datetime import datetime
import logging
import getpass
import re
import rpc
import simplejson
import sys
import os
from ebaysdk.trading import Connection as Trading
from ebaysdk.shopping import Connection as Shopping
import opsystConnection

current_user = getpass.getuser()
this_module_name = os.path.basename(sys.modules['__main__'].__file__)

thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

STORE_CODE = 'rhino'

logs_path = './logs/'
LOG_FILE = os.path.join(logs_path, '%s_ebay_request_tools_%s.log') % (STORE_CODE, datetime.today().strftime('%Y_%m'))
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
store_model = 'sale.store'
DOMAIN = 'api.ebay.com'
Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)


def get_listings():
    store_id = rpc.read(store_model, [['code', '=', STORE_CODE]])
    STORE_ID = store_id[0]
    ebay_trading_api = Trading(domain=DOMAIN, config_file=None, appid=STORE_ID['ebay_app_id'], devid=STORE_ID['ebay_dev_id'], certid=STORE_ID['ebay_cert_id'], token=STORE_ID['ebay_token'], siteid=str(STORE_ID['ebay_site_id']))
    ebay_shopping_api = Shopping(config_file=None, appid=STORE_ID['ebay_app_id'], siteid='100')
    ops_item_ids_res = Connection.odoo_execute("""SELECT name FROM product_listing WHERE store_id = %s AND state = 'active'""" % STORE_ID['id'])
    ops_item_ids = [r['name'] for r in ops_item_ids_res]
    for ind, item in enumerate(ops_item_ids):
        try:
            result = ebay_trading_api.execute('GetItem', {'ItemID': item, 'IncludeItemSpecifics': True}).dict()
        except Exception as e:
            logging.error(e)
            continue
        for i, r in enumerate(result['Item']['ItemSpecifics']['NameValueList']):
            try:
                if type(r['Value']) == str and re.match(r'\d\d-[0-9]+-[0-9]+', r['Value']):
                    logging.warning('%s %s %s', result['Item']['ItemID'], r['Name'], r['Value'])
                    r['Value'] = random.randint(10000000, 100000000)
                    vals_to_push = {'NameValueList': []}
                    for spec in result['Item']['ItemSpecifics']['NameValueList']:
                        vals_to_push['NameValueList'].append({'Name': spec['Name'], 'Value': spec['Value']})
                    item_dict = dict({'ItemID': item, 'ItemSpecifics': vals_to_push})
                    res = ebay_trading_api.execute('ReviseItem', {'Item': item_dict}, list_nodes=[], verb_attrs=None, files=None)
                    logging.info('eBay Response: %s' % res.reply)
            except Exception as e:
                logging.error(e)


def split_on_chunks(lst, num):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(lst), num):
        yield lst[i:i + num]


if __name__ == "__main__":
    get_listings()

# # -*- coding: utf-8 -*-

from ebaysdk.shopping import Connection as Shopping
from ebaysdk.trading import Connection as Trading
import logging
import operator
import itertools
import simplejson
from datetime import datetime
import sys
import os
from ebaysdk.trading import Connection as Trading
from ebaysdk.exception import ConnectionError
import opsystConnection

thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'test.json'), mode='r')
config = simplejson.load(fp)
logs_path = config.get('logs_path')
LOG_FILE = os.path.join(logs_path + 'spec_set_in_ebay___%s.log') % datetime.today().strftime('%Y_%m')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', filename=LOG_FILE, datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger().addHandler(logging.StreamHandler())
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')

Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user,
                                               odoo_db_password=odoo_db_password)

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


def go():
    query = """select name 
               from product_listing
               where state='active'
               AND store_id = 5"""
    res = Connection.odoo_execute(query)
    for rec in res:
        # push_to_ebay(rec['name'])
        push_to_ebay('272740036056')


def push_to_ebay(item):
    vals_to_push = {'NameValueList': []}
    ebay_item = get_item_from_ebay(item)
    if not ebay_item:
        logging.warning('No ebay item listing id = %s', item)
        return
    logging.info('Prepare to push %s', item)
    ebay_spec_list = ebay_item._dict['Item']['ItemSpecifics']['NameValueList']
    for spec in ebay_spec_list:
        if spec['Name'].lower() != 'brand':
            vals_to_push['NameValueList'].append({'Name': spec['Name'], 'Value': spec['Value']})
    vals_to_push['NameValueList'].append({'Name': 'Brand', 'Value': 'Unbranded'})
    item_dict = dict({'ItemID': item})
    item_dict['ItemSpecifics'] = vals_to_push
    res = ebay_trading_api.execute('ReviseItem', {'Item': item_dict}, list_nodes=[], verb_attrs=None, files=None)
    logging.info('eBay Response: %s' % res.reply)


def get_item_from_ebay(listing_name):
    data = {'ItemID': listing_name, 'IncludeItemSpecifics': True}
    ebay_item = ebay_trading_api.execute('GetItem', data, list_nodes=[], verb_attrs=None, files=None)
    return ebay_item


def main():
    go()


if __name__ == "__main__":
    main()

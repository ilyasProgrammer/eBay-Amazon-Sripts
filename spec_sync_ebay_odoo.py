# # -*- coding: utf-8 -*-

"""Sync item specifics from ebay to odoo"""

import xmlrpclib
import psycopg2
import psycopg2.extras
import logging
import operator
import itertools
import rpc
import time
import sys
import os
from ebaysdk.trading import Connection as Trading
from ebaysdk.exception import ConnectionError

# CONN_STR = "dbname='auto-2016-12-21' user='postgres' host='localhost'"
CONN_STR = "dbname='auto-2016-12-21' user='auto' host='localhost' password='1q2w3e4r5t!' sslmode=disable"

OFFSET = int(sys.argv[1])  # starting row in request
LIMIT = int(sys.argv[2])  # quantity of records starting from OFFSET
STORE_CODE = 'visionary'

DONE_LISTINGS_IDS = []
JUST_STARTED = True
RUN = True
DOMAIN = 'api.ebay.com'
UPC_VAL = 'Does Not Apply'
LISTING_SPECIFIC = ['Brand', 'Manufacturer Part Number', 'Warranty', 'UPC']
ALL_LISTING_RECS = []
ALL_ODOO_ATTRS = []
TOTAL = 0
STORE_ID = {}
ebay_api = ''
directory = './logs/' + STORE_CODE
if not os.path.exists(directory):
    os.makedirs(directory)
log_file = directory + '/' + time.strftime("%Y-%m-%d %H:%M:%S") + ' ' + 'spec_sync_odoo_ebay.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S", filemode='a')
logging.getLogger().addHandler(logging.StreamHandler())
listing_model = 'product.listing'
store_model = 'sale.store'
attr_model = 'product.item.specific.attribute'
value_model = 'product.item.specific.value'
product_model = 'product.template'
line_model = 'product.item.specific.line'


def init():
    global ALL_ODOO_ATTRS
    global ALL_LISTING_RECS
    global TOTAL
    global STORE_ID
    global ebay_api
    logging.info('Init')
    store_id = rpc.read(store_model, [['code', '=', STORE_CODE]])
    if not store_id:
        logging.error('Fatal error. No store with name = %s' % STORE_CODE)
        RUN = False
        return
    else:
        STORE_ID = store_id[0]
    logging.info('Store: %s', STORE_ID['name'])

    # check UPC
    upc_id = rpc.search(attr_model, [['name', '=', 'UPC']])
    if not upc_id:
        upc_id = rpc.create(attr_model, {'name': 'UPC', 'listing_specific': True})
        logging.info('Created UPC attribute')
    else:
        upc_id = upc_id[0]
    # Check UPC value
    upc_val_id = rpc.search(value_model, [['name', '=', UPC_VAL], ['item_specific_attribute_id', '=', upc_id]])
    if not upc_val_id:
        upc_val_id = rpc.create(value_model, {'name': UPC_VAL, 'item_specific_attribute_id': upc_id})
        logging.info('%s Created UPC value')
    else:
        upc_val_id = upc_val_id[0]
    logging.info('Read listing_specifics ...')
    # listing_specific_attrs_recs = rpc.read(attr_model, [['listing_specific', '=', True]])
    # SPEC_ATTR_NAMES = [r['name'] for r in listing_specific_attrs_recs]  # will need it to exclude
    # ALL_LISTING_RECS = rpc.search(listing_model, [['state', '=', 'active'], ['store_id', '=', STORE_ID['id']]])
    conn = psycopg2.connect(CONN_STR)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query = """SELECT * from product_item_specific_attribute"""
    cur.execute(query)
    ALL_ODOO_ATTRS = cur.fetchall()
    conn.close()
    # query = """SELECT * from product_listing WHERE state = 'active' AND store_id = %s""" % STORE_ID['id']
    # cur.execute(query)
    # ALL_LISTING_RECS = cur.fetchall()
    # TOTAL = len(ALL_LISTING_RECS)
    ebay_api = Trading(domain=DOMAIN, config_file=None, appid=STORE_ID['ebay_app_id'], devid=STORE_ID['ebay_dev_id'], certid=STORE_ID['ebay_cert_id'], token=STORE_ID['ebay_token'], siteid=str(STORE_ID['ebay_site_id']))


def from_ebay_to_odoo():
    global RUN
    global DONE_LISTINGS_IDS
    conn = psycopg2.connect(CONN_STR)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    step = 1
    query = """SELECT b.id AS atr_id, b.name AS atr_name, a.id AS val_id, a.name AS val_name FROM  product_item_specific_value a
                LEFT JOIN product_item_specific_attribute b
                ON a.item_specific_attribute_id = b.id
                ORDER BY atr_name, val_name"""
    logging.info('Read all odoo attrs ...')
    cur.execute(query)
    attrs = cur.fetchall()
    query = """SELECT ls.name as listing_name, ls.product_tmpl_id, l.id as line_id, l.value_id, l.item_specific_attribute_id,a.name as atr_name,v.name as val_name
                    FROM product_listing as ls
                    JOIN product_item_specific_line as l
                    ON ls.product_tmpl_id = l.product_tmpl_id
                    JOIN product_item_specific_attribute as a
                    ON l.item_specific_attribute_id = a.id
                    JOIN product_item_specific_value as v
                    ON l.value_id = v.id
                    WHERE ls.state = 'active' AND ls.store_id = %s AND a.listing_specific = FALSE
                    ORDER BY  ls.id, ls.product_tmpl_id
                    LIMIT %s OFFSET %s""" % (STORE_ID['id'], LIMIT, OFFSET)
    cur.execute(query)
    logging.info('Read odoo listing lines from %s to %s', OFFSET, LIMIT)
    data_to_ebay = cur.fetchall()
    conn.close()
    grouped_data = []
    for key, items in itertools.groupby(data_to_ebay, operator.itemgetter('listing_name')):
        grouped_data.append(list(items))
    total = len(grouped_data)
    logging.info('Total: %s', total)
    for listing_group in grouped_data:
        item = listing_group[0]['listing_name']
        skipped = 0
        if item in DONE_LISTINGS_IDS:
            skipped += 1
            step += 1
            continue
        logging.info('Skipped: %s', skipped)
        DONE_LISTINGS_IDS.append(item)
        ebay_item = get_item_from_ebay(item)
        if not ebay_item:
            logging.warning('%s/%s No ebay item listing id = %s', step, total, item)
            continue
        logging.info('%s/%s Prepare  %s', step, total, item)
        ebay_attrs_list = ebay_item._dict['Item']['ItemSpecifics']['NameValueList']
        n_atr = 1
        total_atr = len(ebay_attrs_list)
        for eb in ebay_attrs_list:
            if eb['Name'] in LISTING_SPECIFIC:
                n_atr += 1
                continue
            found = False
            logging.info('%s/%s %s/%s Ebay attr = %s value =  %s', step, total, n_atr, total_atr, eb['Name'], eb['Value'])
            for li in listing_group:
                if eb['Name'] == li['atr_name']:
                    found = True
                    if eb['Value'] != li['val_name']:
                        res = get_atr_val(eb['Name'], eb['Value'], attrs)
                        if res[0]:
                            rpc.write(line_model, li['line_id'],
                                      {'item_specific_attribute_id': res[1]['atr_id'],
                                       'value_id': res[1]['val_id']})
                            logging.info('%s/%s %s/%s Updated with ebay existing value %s %s', step, total, n_atr, total_atr,  eb['Name'], eb['Value'])
                            logging.info('%s/%s %s/%s Updated with odoo existing value %s %s', step, total, n_atr, total_atr,  res[1]['atr_name'], res[1]['val_name'])
                        elif res[1] is not False:
                            val_id = rpc.create(value_model,
                                                {'name': str(eb['Value']).replace('[', '').replace(']', '').replace("'", ""),
                                                 'item_specific_attribute_id': res[1]['atr_id']})
                            logging.info('%s/%s %s/%s New value %s %s', step, total, n_atr, total_atr, eb['Name'], eb['Value'])
                            rpc.create(line_model,
                                       {'item_specific_attribute_id': res[1]['atr_id'],
                                        'product_tmpl_id': listing_group[0]['product_tmpl_id'],
                                        'value_id': val_id})
                            logging.info('%s/%s %s/%s Created line %s %s', step, total, n_atr, total_atr,  eb['Name'], eb['Value'])
                    else:
                        logging.info('%s/%s %s/%s Same values %s %s', step, total, n_atr, total_atr,  eb['Name'], eb['Value'])
                        break  # already same
                else:
                    pass  # just continue
            if not found:
                attribute_id = rpc.create(attr_model, {'name': eb['Name']})
                logging.info('%s/%s %s/%s New attr %s', step, total, n_atr, total_atr,  eb['Name'])
                val_id = rpc.create(value_model, {'name': eb['Value'], 'item_specific_attribute_id': attribute_id})
                logging.info('%s/%s %s/%s New val %s', step, total, n_atr, total_atr,  eb['Value'])
                new_line = rpc.create(line_model, {'value_id': val_id, 'product_tmpl_id': listing_group[0]['product_tmpl_id'], 'item_specific_attribute_id': attribute_id})
                logging.info('%s/%s %s/%s New line %s', step, total, n_atr, total_atr, new_line)
            n_atr += 1
        step += 1
    RUN = False
    return


def get_atr_val(atr, val, attrs):
    res = False, False
    for r in attrs:
        if r['atr_name'] == atr:
            if r['val_name'] == val:
                return True, r
        else:
            res = False, r
    return res


def get_item_from_ebay(listing_name):
    data = {'ItemID': listing_name, 'IncludeItemSpecifics': True}
    ebay_item = ebay_api.execute('GetItem', data, list_nodes=[], verb_attrs=None, files=None)
    return ebay_item


def main():
    while RUN:
        logging.info('\n\n\nSTART spec_sync_odoo_ebay.py')
        init()
        try:
            from_ebay_to_odoo()
        except:
            logging.error('Error: %s' % sys.exc_info()[0])
            logging.error('Error: %s' % sys.exc_info()[1])
        logging.warning('Sleep 3 sec and will continue ...')
        time.sleep(3)
    logging.info('Done !')


if __name__ == "__main__":
    main()

# # -*- coding: utf-8 -*-

"""Sync item specifics in odoo and ebay"""

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

# CONN_STR = "dbname='auto_local' user='postgres' host='localhost'"
CONN_STR = "dbname='auto-2016-12-21' user='auto' host='localhost' password='1q2w3e4r5t!' sslmode=disable"

OFFSET = int(sys.argv[1])  # starting row in request
LIMIT = int(sys.argv[2])  # quantity of records starting from OFFSET
STORE_CODE = sys.argv[2]  # like 'revive'

DONE_LISTINGS_IDS = []
JUST_STARTED = True
RUN = True
DOMAIN = 'api.ebay.com'
UPC_VAL = 'Does Not Apply'
LISTING_SPECIFIC = ['Brand', 'Manufacturer Part Number', 'Warranty']
ALL_ODOO_ATTRS = []
TOTAL = 0
STORE_ID = {}
conn = psycopg2.connect(CONN_STR)
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
    # global ALL_LISTING_RECS
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
    # logging.info('Read listing_specifics ...')
    # listing_specific_attrs_recs = rpc.read(attr_model, [['listing_specific', '=', True]])
    # SPEC_ATTR_NAMES = [r['name'] for r in listing_specific_attrs_recs]  # will need it to exclude
    # ALL_LISTING_RECS = rpc.read(listing_model, [['state', '=', 'active'], ['store_id', '=', store_id['id']]])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query = """SELECT * from product_item_specific_attribute"""
    cur.execute(query)
    ALL_ODOO_ATTRS = cur.fetchall()
    # query = """SELECT * from product_listing WHERE state = 'active' AND store_id = %s""" % STORE_ID['id']
    # cur.execute(query)
    # ALL_LISTING_RECS = cur.fetchall()
    # TOTAL = len(ALL_LISTING_RECS)
    ebay_api = Trading(domain=DOMAIN, config_file=None, appid=STORE_ID['ebay_app_id'], devid=STORE_ID['ebay_dev_id'], certid=STORE_ID['ebay_cert_id'], token=STORE_ID['ebay_token'], siteid=str(STORE_ID['ebay_site_id']))


def from_ebay_to_odoo():
    global JUST_STARTED
    global DONE_LISTINGS_IDS
    global RUN
    cnt = 1

    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        for listing_rec in ALL_LISTING_RECS:
            if listing_rec['id'] in DONE_LISTINGS_IDS:
                continue
            DONE_LISTINGS_IDS.append(listing_rec['id'])
            logging.info('%s iteration of %s', cnt, TOTAL)
            logging.info('%s Started proceeding of listing id = %s', cnt, listing_rec['id'])  # Get listing from eBay
            data = {'ItemID': listing_rec['name'], 'IncludeItemSpecifics': True}
            ebay_item = ebay_api.execute('GetItem', data, list_nodes=[], verb_attrs=None, files=None)
            if not ebay_item:
                logging.warning('%s No ebay item listing id = %s', cnt, listing_rec['id'])
                continue
            # Get product from Odoo
            product_id = listing_rec['product_tmpl_id']
            # Add eBay values to product
            ebay_attrs_list = ebay_item._dict['Item']['ItemSpecifics']['NameValueList']
            ebay_attrs_names = [r['Name'] for r in ebay_attrs_list]
            check_all_attrs(ebay_attrs_names)

            item_specific_vals = []
            att_ids_from_listing = []
            total_spec = len(ebay_attrs_list)
            spec_cnt = 1
            for i in ebay_attrs_list:
                if i['Name'] in []:
                    spec_cnt += 1
                    logging.info('%s/%s - %s/%s Item specific. Continue %s ...', cnt, TOTAL, spec_cnt, total_spec, i['Name'])
                    continue
                query = """SELECT l.id, l.product_tmpl_id, l.item_specific_attribute_id, a.name 
                            FROM product_item_specific_line as l
                            LEFT JOIN product_item_specific_attribute as a
                            ON l.item_specific_attribute_id = a.id 
                            WHERE a.name = '%s'
                            """ % str(i['Name'])
                cur.execute(query)
                lines = cur.fetchall()
                line = filter(lambda r: r['product_tmpl_id'] == product_id, lines)
                if len(line) == 1:
                    rpc.write(line_model, line[0]['product_tmpl_id'], {'value_id': value_id['id']})
                    logging.info('%s/%s - %s/%s Updated value for product = %s, %s = %s', cnt, TOTAL, spec_cnt, total_spec, product_id, i['Name'], i['Value'])
                elif len(line) > 1:
                    # if get here then will make it
                    Run = False
                    return
                    # line = filter(lambda r: r['item_specific_attribute_id'] == ??? , line)
                else:
                    attribute_id, value_id = get_attr_value_rec(i['Value'], i['Name'])
                    new_line = rpc.create(line_model, {'value_id': value_id,
                                                       'product_tmpl_id': product_id,
                                                       'item_specific_attribute_id': attribute_id})
                    logging.info('%s/%s - %s/%s Created line using eBay data %s %s = %s', cnt, TOTAL, spec_cnt, total_spec, new_line, i['Name'], i['Value'])
                spec_cnt += 1
            # Upload lines to eBay except listing_specific and ones already there
            attrs_to_ebay = {'NameValueList': []}
            cnt += 1
    except:
        logging.error('Error: %s' % sys.exc_info()[0])
        logging.error('Error: %s' % sys.exc_info()[1])
        return
    RUN = False


def check_all_attrs(ebay_attrs_names):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # Check if attribute exist
    logging.info('Check all attributes for item ...')
    names = [r['name'] for r in ALL_ODOO_ATTRS]
    for i in ebay_attrs_names:
        if i not in names:
            attribute_id = rpc.create(attr_model, {'name': i['Name']})
            logging.info('Created item attribute using eBay data: %s' % i['Name'])


def get_attr_value_rec(name, attr_name):
    # Check if value exist for this attribute
    logging.info('Search value %s ...', name)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    attr = filter(lambda r: r['name'] == attr_name, ALL_ODOO_ATTRS)[0]  # must present. I checked all attr already
    query = """SELECT * from product_item_specific_value 
                          WHERE name = '%s'
                          AND item_specific_attribute_id = %s """ % (name, attr['id'])
    cur.execute(query)
    value_id = cur.fetchall()
    if not value_id:
        value_id = rpc.create(value_model, {'name': name, 'item_specific_attribute_id': attr['id']})
        logging.info('Created value using eBay data: %s', name)
    else:
        value_id = value_id[0]
    return attr['id'], value_id['id']


def odoo_to_ebay():
    global RUN
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    step = 1
    query = """SELECT ls.name as listing_name, ls.product_tmpl_id, l.value_id, l.item_specific_attribute_id,a.name as atr_name,v.name as val_name
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
    grouped_data = []
    for key, items in itertools.groupby(data_to_ebay, operator.itemgetter('listing_name')):
        grouped_data.append(list(items))
    total = len(data_to_ebay)
    logging.info('Total: %s', total)
    for listing_group in grouped_data:
        item = listing_group[0]['listing_name']
        skipped = ''
        if item in DONE_LISTINGS_IDS:
            skipped += str(item + ', ')
            step += 1
            continue
        logging.info('Skipped: %s: %s', len(skipped), skipped)
        DONE_LISTINGS_IDS.append(item)
        ebay_item = get_item_from_ebay(item)
        if not ebay_item:
            logging.warning('%s/%s No ebay item listing id = %s', step, total, item)
            continue
        logging.info('%s/%s Prepare to push %s', step, total, item)
        ebay_attrs_list = ebay_item._dict['Item']['ItemSpecifics']['NameValueList']
        ebay_attrs_names = [r['Name'] for r in ebay_attrs_list]
        push_to_ebay(listing_group, ebay_attrs_list, item, ebay_attrs_names)
        step += 1
    RUN = False
    return


def push_to_ebay(listing_group, ebay_attrs_list, item, ebay_attrs_names):
    vals_to_push = {'NameValueList': []}
    # add vals from odoo that is not in ebay
    for row in listing_group:
        if row['atr_name'] not in ebay_attrs_names:
            vals_to_push['NameValueList'].append({'Name': esc(row['atr_name']), 'Value': esc(row['val_name'])})
    # add ebay val
    for row in ebay_attrs_list:
        vals_to_push['NameValueList'].append({'Name': esc(row['Name']), 'Value': esc(row['Value'])})
    item_dict = dict({'ItemID': item})
    item_dict['ItemSpecifics'] = vals_to_push
    logging.info('Sending item data to eBay %s' % item_dict)
    res = ebay_api.execute('ReviseItem', {'Item': item_dict}, list_nodes=[], verb_attrs=None, files=None)
    logging.info('eBay Response: %s' % res.reply)


def get_item_from_ebay(listing_name):
    data = {'ItemID': listing_name, 'IncludeItemSpecifics': True}
    ebay_item = ebay_api.execute('GetItem', data, list_nodes=[], verb_attrs=None, files=None)
    return ebay_item


def esc(st):
    return st.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&apos;')


def main():
    while RUN:
        logging.info('\n\n\nSTART spec_sync_odoo_ebay.py')
        logging.info('Offset = %s Limit = %s', OFFSET, LIMIT)
        # init()
        # from_ebay_to_odoo()
        init()
        try:
            odoo_to_ebay()
        except:
            logging.error('Error: %s' % sys.exc_info()[0])
            logging.error('Error: %s' % sys.exc_info()[1])
        logging.warning('Sleep 3 sec and will continue ...')
        time.sleep(3)
    logging.info('Done !')


if __name__ == "__main__":
    main()

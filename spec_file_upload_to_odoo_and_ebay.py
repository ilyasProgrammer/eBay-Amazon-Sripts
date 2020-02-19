# # -*- coding: utf-8 -*-

"""Uploads item specific attributes from Odoo to eBay"""

import xmlrpclib
import logging
import csv
import time
import sys
from ebaysdk.trading import Connection as Trading
from ebaysdk.exception import ConnectionError

START_LINE = 1
RUN = True
STORE_NAME = 'Visionary Auto Parts'
UPLOAD_FILE = "FileExchange_Response_41660051.csv"
DOMAIN = 'api.ebay.com'
UPC_VAL = 'Does Not Apply'

listing_model = 'product.listing'
store_model = 'sale.store'
attr_model = 'product.item.specific.attribute'
value_model = 'product.item.specific.value'
product_model = 'product.template'
line_model = 'product.item.specific.line'

# url = "http://localhost:8069"
# db = 'auto_local'
# username = 'ajporlante@gmail.com'
# password = '123'
url = 'http://opsyst.com'
db = 'auto-2016-12-21'
username = 'i.rakhimkulov@vertical4.com'
password = 'fafa321'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='/tmp/myapp.log',
                    filemode='w')


def upload_items():
    global START_LINE
    global RUN
    cnt = 1
    store_id = rpc_read(store_model, [['name', '=', STORE_NAME]])
    if not store_id:
        logging.error('Fatal error. No store with name = %s' % STORE_NAME)
        RUN = False
        return
    else:
        store_id = store_id[0]
    # check UPC
    upc_id = rpc_search(attr_model, [['name', '=', 'UPC']])
    if not upc_id:
        upc_id = rpc_create(attr_model, {'name': 'UPC', 'listing_specific': True})
        logging.info('%s Created UPC attribute', cnt)
    else:
        upc_id = upc_id[0]
    # Check UPC value
    upc_val_id = rpc_search(value_model, [['name', '=', UPC_VAL], ['item_specific_attribute_id', '=', upc_id]])
    if not upc_val_id:
        upc_val_id = rpc_create(value_model, {'name': UPC_VAL, 'item_specific_attribute_id': upc_id})
        logging.info('%s Created UPC value', cnt)
    else:
        upc_val_id = upc_val_id[0]
    ebay_api = Trading(domain=DOMAIN,
                       config_file=None,
                       appid=store_id['ebay_app_id'],
                       devid=store_id['ebay_dev_id'],
                       certid=store_id['ebay_cert_id'],
                       token=store_id['ebay_token'],
                       siteid=str(store_id['ebay_site_id']))
    try:
        with open(UPLOAD_FILE) as tsv:
            reader = csv.DictReader(tsv)
            for ind, row in enumerate(reader):
                if reader.line_num < START_LINE:
                    continue
                cnt = reader.line_num
                f_item_id = row['Item ID'].strip()
                logging.info('\n%s Line. Item: %s' % (cnt, f_item_id))
                # Get item listing from Odoo
                item_id = rpc_read(listing_model, [['name', '=', f_item_id]])
                if not item_id:
                    logging.warning('%s No item with id = %s' % (cnt, f_item_id))
                    continue
                else:
                    item_id = item_id[0]
                # Get item from eBay
                data = {'ItemID': f_item_id, 'IncludeItemSpecifics': True}
                ebay_item = ebay_api.execute('GetItem', data, list_nodes=[], verb_attrs=None, files=None)
                if not ebay_item:
                    logging.warning('%s No ebay item with id = %s' % (cnt, f_item_id))
                    continue
                # Get product from Odoo
                product_id = rpc_read(product_model, [['id', '=', item_id['product_tmpl_id'][0]]])
                if not product_id:
                    logging.warning('%s No product template with id = %s' % (cnt, item_id['product_tmpl_id']))
                    continue
                else:
                    product_id = product_id[0]
                # Add eBay values to product
                item_specific_list = ebay_item._dict['Item']['ItemSpecifics']['NameValueList']
                item_specific_vals = []
                att_ids_from_listing = []
                for i in item_specific_list:
                    # Check if attribute exist
                    logging.info('%s Getting attribute %s from odoo ...', cnt, i['Name'])
                    attribute_id = rpc_read(attr_model, [['name', '=', i['Name']]])
                    if not attribute_id:
                        attribute_id = rpc_create(attr_model, {'name': i['Name']})
                        logging.info('%s Created item attribute using eBay data: %s %s' % (cnt, attribute_id, i['Name']))
                    else:
                        attribute_id = attribute_id[0]
                    # Skip listing_specific
                    if attribute_id['listing_specific']:
                        continue
                    # Check if value exist for this attribute
                    logging.info('%s Getting value %s from odoo ...', cnt, i['Value'])
                    value_id = rpc_search(value_model, [['name', '=', i['Value']], ['item_specific_attribute_id', '=', attribute_id['id']]])
                    if not value_id:
                        value_id = rpc_create(value_model, {'name': i['Value'], 'item_specific_attribute_id': attribute_id['id']})
                        logging.info('%s Created item specific value using eBay data: %s %s = %s' % (cnt, value_id, i['Name'], i['Value']))
                    else:
                        value_id = value_id[0]
                    item_specific_vals.append({'value_id': value_id, 'item_specific_attribute_id': attribute_id['id']})
                    att_ids_from_listing.append(attribute_id['id'])
                    # Check if line exist for attribute
                    logging.info('%s Getting line %s = %s from odoo ...', cnt, i['Name'], i['Value'])
                    line = rpc_search(line_model, [['product_tmpl_id', '=', product_id['id']], ['item_specific_attribute_id', '=', attribute_id['id']]])
                    if line:
                        rpc_write(line_model, line[0], {'value_id': value_id})
                        logging.info('%s Updated value for product = %s, %s = %s' % (cnt, product_id['id'], i['Name'], i['Value']))
                    else:
                        new_line = rpc_create(line_model, {'value_id': value_id, 'product_tmpl_id': product_id['id'], 'item_specific_attribute_id': attribute_id['id']})
                        logging.info('%s Created line using eBay data %s %s = %s' % (cnt, new_line, i['Name'], i['Value']))
                # Upload lines to eBay except listing_specific and ones already there
                item_specifics = {'NameValueList': []}
                # Get product from Odoo again with new attrs
                logging.info('%s Getting product %s again from odoo ...', cnt, item_id['product_tmpl_id'][0])
                product_id = rpc_read(product_model, [['id', '=', item_id['product_tmpl_id'][0]]])
                if not product_id:
                    logging.warning('%s No product template with id = %s' % (cnt, item_id['product_tmpl_id']))
                    continue
                else:
                    product_id = product_id[0]
                listing_specific_attrs_ids = rpc_read(attr_model, [['listing_specific', '=', True]])
                listing_specific_attrs_names = [r['name'] for r in listing_specific_attrs_ids]
                ebay_attrs_names = [r['Name'] for r in item_specific_list]
                product_attrs_names = []
                for line_id in product_id['item_specific_line_ids']:
                    line = rpc_read(line_model, [['id', '=', line_id]])[0]
                    attr = rpc_read(attr_model, [['id', '=', line['item_specific_attribute_id'][0]]])[0]
                    product_attrs_names.append(attr['name'])
                    if (attr['listing_specific'] is True) or (attr['name'] in ebay_attrs_names):
                        continue
                    val = rpc_read(value_model, [['id', '=', line['value_id'][0]]])[0]
                    item_specifics['NameValueList'].append({'Name': attr['name'], 'Value': val['name']})
                # Upload ebay listing_specific values back
                for ebay_line in item_specific_list:
                    if ebay_line['Name'] in listing_specific_attrs_names:
                        item_specifics['NameValueList'].append({'Name': ebay_line['Name'], 'Value': ebay_line['Value']})
                # Add UPC
                if 'UPC' not in product_attrs_names:
                    logging.info('%s Adding UPC', cnt)
                    item_specifics['NameValueList'].append({'Name': 'UPC', 'Value': UPC_VAL})
                    # line = rpc_search(line_model, [['product_tmpl_id', '=', product_id['id']], ['item_specific_attribute_id', '=', upc_id]])
                    # if line:
                    #     logging.error('%s UPC must present from the beginning ! That is strange.')
                    # else:
                    #     new_line = rpc_create(line_model, {'value_id': upc_val_id, 'product_tmpl_id': product_id['id'], 'item_specific_attribute_id': upc_id})
                    #     logging.info('%s Created UPC line for product %s', cnt, product_id['name'])
                item_dict = {'ItemID': item_id['name']}
                item_dict['ItemSpecifics'] = item_specifics
                logging.info('%s Sending item data to eBay %s', cnt, item_specifics)
                ebay_api.execute('ReviseItem', {'Item': item_dict}, list_nodes=[], verb_attrs=None, files=None)
                logging.info('%s Sent item data to eBay OK', cnt)
    except:
        START_LINE = cnt
        logging.error('Error: %s' % sys.exc_info()[0])
        logging.error('Error: %s' % sys.exc_info()[1])
        return
    RUN = False
    logging.info('Done!')


def rpc_read(model, domain):
    return models.execute_kw(db, uid, password, model, 'search_read', [domain])


def rpc_search(model, domain):
    return models.execute_kw(db, uid, password, model, 'search', [domain])


def rpc_write(model, record_id, data):
    return models.execute_kw(db, uid, password, model, 'write', [[record_id], data])


def rpc_create(model, data):
    return models.execute_kw(db, uid, password, model, 'create', [data])


def main():
    while RUN:
        upload_items()
        logging.warning('Sleep 120 sec and will continue ...')
        time.sleep(5)


if __name__ == "__main__":
    main()

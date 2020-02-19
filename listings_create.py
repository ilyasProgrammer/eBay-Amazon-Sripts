# -*- coding: utf-8 -*-

"""Import listings from file to Odoo"""

import logging
import rpc
import time
from random import shuffle
import pymssql
import re
import sys
from collections import defaultdict
START_LINE = ''
RUN = True
UPLOAD_FILE = './src/amazon.txt'
STORE_NAME = 'Sinister Auto Parts'
log_file = './logs/'+time.strftime("%Y-%m-%d %H:%M:%S")+' '+__file__.rsplit('/', 1)[1]+'.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s.%(msecs)03d %(levelname)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S", filemode='a')
logging.getLogger().addHandler(logging.StreamHandler())
listing_model = 'product.listing'
store_model = 'sale.store'
attr_model = 'product.item.specific.attribute'
value_model = 'product.item.specific.value'
product_model = 'product.template'
line_model = 'product.item.specific.line'


def go():
    global START_LINE
    global RUN
    cnt = 1
    store_id = rpc.search(store_model, [['name', '=', STORE_NAME]])
    if not store_id:
        logging.error('Fatal error. No store with name = %s' % STORE_NAME)
        RUN = False
        return
    else:
        store_id = store_id[0]
    try:
        with open(UPLOAD_FILE) as f:
            for ind, line in enumerate(f):
                cnt = ind
                l_product = line.split(' ')[-1].strip().replace('\n', '')
                l_sku = line.split(' ')[0].strip().replace('\n', '')
                # Get product from Odoo
                product_id = rpc.search(product_model, [['name', '=', l_product]])
                if not product_id:
                    logging.error('%s No product template with id = %s', cnt, l_product)
                    continue
                else:
                    product_id = product_id[0]
                # SKU exist ?
                listing_id = rpc.search(listing_model, [['name', '=', l_sku], ['store_id', '=', store_id]])
                if listing_id:
                    listing_id = listing_id[0]
                    logging.info('%s Listing %s exist. Overwrite. Product:%s %s, SKU:%s', cnt, listing_id, product_id, l_product, l_sku)
                    res = rpc.write(listing_model, listing_id, {'name': l_sku, 'product_tmpl_id': product_id, 'store_id': store_id})
                else:
                    new_listing_id = rpc.create(listing_model, {'name': l_sku, 'product_tmpl_id': product_id, 'store_id': store_id})
                    logging.info('%s New listing: %s. Product:%s %s, SKU:%s', cnt, new_listing_id, product_id, l_product, l_sku)
    except:
        START_LINE = cnt
        logging.error('Error: %s' % sys.exc_info()[0])
        logging.error('Error: %s' % sys.exc_info()[1])
        return
    RUN = False
    logging.info('Done!')


if __name__ == "__main__":
    while RUN:
        logging.info('\n\n\nSTART %s', __file__.rsplit('/', 1)[1])
        go()
        logging.warning('Sleep 15 sec and will continue ...')
        time.sleep(15)
    logging.info('Done !')





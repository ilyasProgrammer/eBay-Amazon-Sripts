# -*- coding: utf-8 -*-

"""Import listings from file to Odoo"""

import logging
import rpc
import time
from random import shuffle
import csv
from csv import excel_tab
import re
import sys
from collections import defaultdict

START_LINE = 0
RUN = True
UPLOAD_FILE = '/home/ra/Downloads/lst7.csv'
STORE_CODE = 'visionary'
log_file = './logs/'+time.strftime("%Y-%m-%d %H:%M:%S")+' '+__file__.rsplit('/', 1)[1]+'.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s.%(msecs)03d %(levelname)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S", filemode='a')
logging.getLogger().addHandler(logging.StreamHandler())
listing_model = 'product.listing'
store_model = 'sale.store'
product_model = 'product.template'


def go():
    global START_LINE
    outfile = open('out', 'wb')
    global RUN
    cnt = 1
    store_id = rpc.search(store_model, [['code', '=', STORE_CODE]])
    if not store_id:
        logging.error('Fatal error. No store with name = %s' % STORE_CODE)
        RUN = False
        return
    else:
        store_id = store_id[0]
    try:
        # with open('/home/ra/Downloads/vwh.csv', 'rb') as f:
        #     reader = csv.reader(f)
        #     vwh = [row[0] for row in reader]
        # with open(UPLOAD_FILE, 'rb') as f:
        #     reader = csv.reader(f)
        #     u = [row[0] for row in reader]
        # for r in vwh:
        #     if r not in u:
        #         print r
        #     continue
        with open(UPLOAD_FILE) as tsv:
            reader = csv.DictReader(tsv)
            for ind, row in enumerate(reader):
                cnt = ind
                lad = row['LAD'].strip()
                name = row['name'].strip()
                # Get product from Odoo
                product_id = rpc.search(product_model, [['name', '=', lad]])
                if not product_id:
                    logging.error('%s No product template with id = %s', cnt, lad)
                    continue
                else:
                    product_id = product_id[0]
                # SKU exist ?
                listing_id = rpc.search(listing_model, [['name', '=', name],
                                                        ['store_id', '=', store_id],
                                                        ['product_tmpl_id', '=', product_id],
                                                        ['state', '=', 'active']])
                lst_vals = {'name': name,
                            'product_tmpl_id': product_id,
                            'brand_id': 36,
                            'store_id': store_id}
                if listing_id:
                    # listing_id = listing_id[0]
                    rpc.write(listing_model, listing_id[0], lst_vals)
                    logging.info('%s Listing %s exist. %s, name:%s', cnt, listing_id, lad, name)
                    outfile.write('%s\n' % str(listing_id[0]))
                    # res = rpc.write(listing_model, listing_id, lst_vals)
                else:
                    new_listing_id = rpc.create(listing_model, lst_vals)
                    logging.info('%s New listing: %s. Product:%s %s, name:%s', cnt, new_listing_id, product_id, lad, name)
    except Exception as e:
        START_LINE = cnt
        logging.error('Error: %s' % sys.exc_info()[0])
        logging.error('Error: %s' % sys.exc_info()[1])
        return
    outfile.close()
    RUN = False
    logging.info('Done with no exceptions')


if __name__ == "__main__":
    while RUN:
        logging.info('\n\n\nSTART %s', __file__.rsplit('/', 1)[1])
        go()
        logging.warning('Sleep 15 sec and will continue ...')
        time.sleep(15)
    logging.info('Done !')

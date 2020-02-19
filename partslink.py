# -*- coding: utf-8 -*-

"""Import partslink nubmers from MSSQL to Odoo"""

import logging
import rpc
import time
from random import shuffle
import pymssql
import re
import sys
from collections import defaultdict
START_LINE = ''
JUST_STARTED = True
RUN = True
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
    global JUST_STARTED
    global START_LINE
    global RUN
    curr_id = ''
    psn = rpc.search(attr_model, [['name', '=', 'Partslink Number']])[0]
    pn = rpc.search(attr_model, [['name', '=', 'Partlink Number']])[0]
    conn = pymssql.connect('192.169.152.87', 'UsmanRaja', 'Nop@ss786', 'AutoPlus')
    cursor = conn.cursor(as_dict=True)

    query = """SELECT INV.PartNo, INTE.PartNo as Partslink
               FROM Inventory INV
               LEFT JOIN InventoryPiesINTE INTE on INTE.InventoryID = INV.InventoryID
               WHERE INV.MfgID = 1 AND INTE.BrandID = 'FLQV' AND INTE.PartNo <>''"""
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    newlist = sorted(results, key=lambda k: k['PartNo'])
    nice_dict = defaultdict(str)
    for d_inner in newlist:
        id_ = d_inner['PartNo']
        nice_dict[id_] += d_inner['Partslink'] + ' , '
    for k, v in nice_dict.iteritems():
        nice_dict[k] = v[:-3]
    for k, v in nice_dict.iteritems():
        if JUST_STARTED and len(START_LINE) != 0:
            if START_LINE != k:
                continue
            else:
                JUST_STARTED = False
                continue
        try:
            curr_id = k
            product_id = rpc.search(product_model, [['name', '=', k]])
            if not product_id:
                logging.error('No product in odoo: %s', k)
                continue
            else:
                product_id = product_id[0]
            # PSN
            psn_val_id = rpc.search(value_model, [['name', '=', v], ['item_specific_attribute_id', '=', psn]])
            if not psn_val_id:
                psn_val_id = rpc.create(value_model, {'name': v, 'item_specific_attribute_id': psn})
                logging.info('New psn value created: %s', v)
            else:
                psn_val_id = psn_val_id[0]
            psn_line_id = rpc.search(line_model, [['product_tmpl_id', '=', product_id], ['item_specific_attribute_id', '=', psn]])
            if not psn_line_id:
                psn_line_id = rpc.create(line_model,  {'value_id': psn_val_id, 'product_tmpl_id': product_id, 'item_specific_attribute_id': psn})
                logging.info('New psn line created: %s', psn_line_id)
            else:
                logging.info('Already has psn attribute. Need to concatenate?')
                psn_line_id = rpc.read(line_model, [['product_tmpl_id', '=', product_id], ['item_specific_attribute_id', '=', psn]])
                psn_line_value_id = psn_line_id[0]['value_id'][0]
                psn_line_value_rec = rpc.read(value_model, [['id', '=', psn_line_value_id]])[0]
                if psn_line_value_rec['name'] == v:
                    logging.info('Same psn values')
                elif psn_line_value_rec['name'] in v:
                    res = rpc.delete(line_model, [psn_line_id[0]['id']])
                    logging.info('Deleted old psn line that was integrated: %s', psn_line_id)
                    new_psn_line_id = rpc.create(line_model, {'value_id': psn_val_id, 'product_tmpl_id': product_id, 'item_specific_attribute_id': psn})
                    logging.info('New psn line concatenated and created: %s', new_psn_line_id)
                else:
                    v += ', ' + psn_line_value_rec['name']
                    res = rpc.delete(line_model, [psn_line_id[0]['id']])
                    logging.info('Deleted old psn line that was integrated: %s', psn_line_id)
                    new_psn_line_id = rpc.create(line_model, {'value_id': psn_val_id, 'product_tmpl_id': product_id, 'item_specific_attribute_id': psn})
                    logging.info('New psn line concatenated and created: %s', new_psn_line_id)
            # PN
            pn_val_id = rpc.search(value_model, [['name', '=', v], ['item_specific_attribute_id', '=', pn]])
            if not pn_val_id:
                pn_val_id = rpc.create(value_model, {'name': v, 'item_specific_attribute_id': pn})
                logging.info('New pn value created: %s', v)
            else:
                pn_val_id = pn_val_id[0]
            pn_line_id = rpc.search(line_model, [['product_tmpl_id', '=', product_id], ['item_specific_attribute_id', '=', pn]])
            if not pn_line_id:
                pn_line_id = rpc.create(line_model, {'value_id': pn_val_id, 'product_tmpl_id': product_id, 'item_specific_attribute_id': pn})
                logging.info('New pn line created: %s', pn_line_id)
            else:
                logging.info('Already has pn attribute. Need to concatenate?')
                pn_line_id = rpc.read(line_model, [['product_tmpl_id', '=', product_id], ['item_specific_attribute_id', '=', pn]])
                pn_line_value_id = pn_line_id[0]['value_id'][0]
                pn_line_value_rec = rpc.read(value_model, [['id', '=', pn_line_value_id]])[0]
                if pn_line_value_rec['name'] == v:
                    logging.info('Same pn values')
                elif pn_line_value_rec['name'] in v:
                    res = rpc.delete(line_model, [pn_line_id[0]['id']])
                    logging.info('Deleted old pn line that was integrated: %s', pn_line_id)
                    new_pn_line_id = rpc.create(line_model, {'value_id': pn_val_id, 'product_tmpl_id': product_id, 'item_specific_attribute_id': pn})
                    logging.info('New pn line concatenated and created: %s', new_pn_line_id)
                else:
                    v += ', ' + pn_line_value_rec['name']
                    res = rpc.delete(line_model, [pn_line_id[0]['id']])
                    logging.info('Deleted old pn line that was integrated: %s', pn_line_id)
                    new_pn_line_id = rpc.create(line_model, {'value_id': pn_val_id, 'product_tmpl_id': product_id, 'item_specific_attribute_id': pn})
                    logging.info('New pn line concatenated and created: %s', new_pn_line_id)
        except:
            START_LINE = curr_id
            logging.error('Error: %s' % sys.exc_info()[0])
            logging.error('Error: %s' % sys.exc_info()[1])
            return
    RUN = False
    JUST_STARTED = False
    logging.info('Done!')


if __name__ == "__main__":
    while RUN:
        logging.info('\n\n\nSTART %s', __file__.rsplit('/', 1)[1])
        go()
        logging.warning('Sleep 15 sec and will continue ...')
        time.sleep(15)
    logging.info('Done !')





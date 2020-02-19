# -*- coding: utf-8 -*-

import rpc
import logging
from datetime import datetime
import os
import csv
from csv import excel_tab

skip_orders = ['111-0668095-4201000',
               '114-6008789-3365005',
               '111-0426768-6694643',
               '112-6841686-2318636',
               '113-4513669-4409857',
               '112-5122087-0873030',
               '113-9248250-4215445',
               '112-6407145-4893808',
               '111-2423342-7681013',
               '114-5127014-5454624',
               '113-3030524-0543459',
               '111-0841239-5999436',
               '111-0330861-6193837',
               '113-5299470-3515433',
               '111-1982270-0308226',
               '111-0365420-1404216',
               '112-3769074-7998612',
               '112-1119015-6029000',
               '111-3734396-2439446',
               '112-9013490-2063411',
               '112-6060673-2614638',
               '113-5419805-2506668',
               '113-1231135-4757866',
               '111-4936272-9232217',
               '111-0442419-4829009']
src_file = '/home/ra/Downloads/orders messed up.csv'
LOG_FILE = os.path.join('complete_kit_orders_%s.log') % datetime.today().strftime('%Y_%m_%s')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)


def go():
    with open(src_file) as tsv:
        reader = csv.DictReader(tsv, dialect=excel_tab)
        i = 0
        for ind, row in enumerate(reader):
            logging.info( i)
            i += 1
            # if row['Order ID'] in skip_orders:
            #     logging.info( 'Skipped %s' % row['Order ID'])
            #     continue
            order_id = rpc.search('sale.order', [['web_order_id', '=', row['Order ID']]])
            if not order_id:
                logging.info( 'Cant find %s' % row['Order ID'])
                continue
            if len(order_id) > 1:
                logging.info( 'Too many orders %s' % row['Order ID'])
                continue
            order = rpc.read('sale.order', [['web_order_id', '=', row['Order ID']]])[0]
            ml1 = rpc.search('product.product', [['name', '=', row['Missing LAD 1'].strip()]])
            if not ml1:
                logging.info( 'Cant find ml1 %s' % row['Missing LAD 1'])
                continue
            order_vals = {'web_order_id': row['Order ID'],
                          'store_id': 7,
                          'amz_order_type': 'normal',
                          'partner_id': order['partner_id'][0],
                          'payment_term_id': 1}
            new_order = rpc.create('sale.order', order_vals)
            logging.info( 'New SO %s' % new_order)
            initial_line = rpc.read('sale.order.line', [['id', '=', order['order_line'][0]]])[0]
            line_vals = {
                'product_id': ml1[0],
                'order_id': new_order,
                'price_unit': 0,
                'product_uom_qty': initial_line['product_uom_qty']
            }
            new_line = rpc.create('sale.order.line', line_vals)
            logging.info( 'New line %s' % new_line)

            if row['Missing LAD 2'].strip():
                ml2 = rpc.search('product.product', [['name', '=', row['Missing LAD 2']]])
                if not ml2:
                    logging.info( 'Cant find ml2 %s' % row['Missing LAD 2'])
                else:
                    line_vals = {
                        'product_id': ml2[0],
                        'order_id': new_order,
                        'price_unit': 0,
                        'product_uom_qty': initial_line['product_uom_qty']
                    }
                    new_line = rpc.create('sale.order.line', line_vals)
                    logging.info( new_line)

            if row['Missing LAD 3'].strip():
                ml3 = rpc.search('product.product', [['name', '=', row['Missing LAD 3']]])
                if not ml3:
                    logging.info( 'Cant find ml3 %s' % row['Missing LAD 3'])
                else:
                    line_vals = {
                        'product_id': ml3[0],
                        'order_id': new_order,
                        'price_unit': 0,
                        'product_uom_qty': initial_line['product_uom_qty']
                    }
                    new_line = rpc.create('sale.order.line', line_vals)
                    logging.info( new_line)

            if row['Missing LAD 4'].strip():
                ml4 = rpc.search('product.product', [['name', '=', row['Missing LAD 4']]])
                if not ml4:
                    logging.info( 'Cant find ml4 %s' % row['Missing LAD 4'])
                else:
                    line_vals = {
                        'product_id': ml4[0],
                        'order_id': new_order,
                        'price_unit': 0,
                        'product_uom_qty': initial_line['product_uom_qty']
                    }
                    new_line = rpc.create('sale.order.line', line_vals)
                    logging.info( new_line)

            if row['Missing LAD 5'].strip():
                ml5 = rpc.search('product.product', [['name', '=', row['Missing LAD 5']]])
                if not ml4:
                    logging.info( 'Cant find ml5 %s' % row['Missing LAD 5'])
                else:
                    line_vals = {
                        'product_id': ml5[0],
                        'order_id': new_order,
                        'price_unit': 0,
                        'product_uom_qty': initial_line['product_uom_qty']
                    }
                    new_line = rpc.create('sale.order.line', line_vals)
                    logging.info( new_line)

            if row['Missing LAD 6'].strip():
                ml6 = rpc.search('product.product', [['name', '=', row['Missing LAD 6']]])
                if not ml6:
                    logging.info( 'Cant find ml6 %s' % row['Missing LAD 6'])
                else:
                    line_vals = {
                        'product_id': ml6[0],
                        'order_id': new_order,
                        'price_unit': 0,
                        'product_uom_qty': initial_line['product_uom_qty']
                    }
                    new_line = rpc.create('sale.order.line', line_vals)
                    logging.info( new_line)


if __name__ == "__main__":
    go()

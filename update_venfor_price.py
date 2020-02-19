# # -*- coding: utf-8 -*-
"""Update product.supplierinfo data with"""

import logging
import rpc
import csv
import os
import simplejson
from csv import excel_tab
import opsystConnection

thisdir = os.path.abspath(os.path.dirname(__file__))
if 1:
    fp = open(os.path.join(thisdir, 'test.json'), mode='r')
else:
    fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

logs_path = config.get('logs_path')
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
CONN_STR = "dbname='auto-2016-12-21' user='postgres' host='localhost'"
Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)


def main():
    UPLOAD_FILE = "/pd/auto/auto_misc_scripts/src/Complete Price List_Period4_PL16.csv"
    START_LINE = 0
    VENDOR_ID = 9  # 9-PFG
    with open(UPLOAD_FILE) as tsv:
        reader = csv.DictReader(tsv, dialect=excel_tab)
        for ind, row in enumerate(reader):
            if reader.line_num < START_LINE:
                continue
            cnt = reader.line_num
            pfg_sku = row['sku'].strip()
            # sku = Connection.autoplus_execute("""
            #              SELECT INV.PartNo AS partno
            #              FROM Inventory INV
            #              LEFT JOIN InventoryAlt ALT on ALT.InventoryID = INV.InventoryID
            #              LEFT JOIN Inventory INV2 on ALT.InventoryIDAlt = INV2.InventoryID
            #              WHERE INV.MfgID = 1 AND INV2.MfgID in (16,35,36,37,38,39) AND INV2.PartNo = '%s'
            #         """ % pfg_sku)
            # if sku:
            #     sku = sku[0]['partno']
            logging.info('%s Line. Item: %s', cnt, pfg_sku)
            # prod = rpc.search('product.template', [('name', '=', sku)])
            # if prod:
            # inf = rpc.search('product.supplierinfo', [('product_code', '=', prod[0]), ('name', '=', VENDOR_ID)])
            inf = rpc.search('product.supplierinfo', [('product_code', '=', pfg_sku), ('name', '=', VENDOR_ID)])
            if inf:
                for r in inf:
                    rpc.write('product.supplierinfo', int(r), {'price': float(row['b2b_price16'].replace(',', '.'))})
                    logging.info('%s Price updated. Item: %s Price: %s PFG_SKU: %s', cnt, r, row['b2b_price16'], pfg_sku)
            else:
                logging.warning("No info %s", pfg_sku)
            # else:
            #     logging.warning("No product %s", sku)


if __name__ == "__main__":
    main()

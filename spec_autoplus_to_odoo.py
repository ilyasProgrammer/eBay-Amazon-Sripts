# # -*- coding: utf-8 -*-

import pymssql
import psycopg2
import psycopg2.extras
import logging
import rpc
import time
import sys
import os

CONN_STR = "dbname='auto-2016-12-21' user='postgres' host='localhost'"
# CONN_STR = "dbname='auto' user='postgres' host='localhost'"
# CONN_STR = "dbname='auto-2016-12-21' user='auto' host='localhost' password='1q2w3e4r5t!' sslmode=disable"

# OFFSET = int(sys.argv[1])  # starting row in request
# LIMIT = int(sys.argv[2])  # quantity of records starting from OFFSET

RUN = True
directory = './logs/'
if not os.path.exists(directory):
    os.makedirs(directory)
log_file = directory + '/' + time.strftime("%Y-%m-%d %H:%M:%S") + ' ' + 'spec_autoplus_to_odoo.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S", filemode='a')
logging.getLogger().addHandler(logging.StreamHandler())
listing_model = 'product.listing'
store_model = 'sale.store'
attr_model = 'product.item.specific.attribute'
value_model = 'product.item.specific.value'
product_model = 'product.template'
line_model = 'product.item.specific.line'
# TODO: Certifications,
# DONE:
AUTOPLUS_ATTR = 'Material'
ATTR = 'Material'


def autoplus_to_odoo():
    global RUN
    conn = psycopg2.connect(CONN_STR)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    attr_id = rpc.read(attr_model, [['name', '=', ATTR]])
    mssql_conn = pymssql.connect('192.169.152.87', 'UsmanRaja', 'Nop@ss786', 'AutoPlus')

    mssql_cur = mssql_conn.cursor(as_dict=True)
    query = """SELECT INV2.PartNo , SPEC.Specification, SPEC.Description 
                FROM USAPFitment.dbo.Specifications SPEC
                LEFT JOIN USAPFitment.dbo.PartNumbers PN ON SPEC.PartID = PN.PartID
                LEFT JOIN Inventory INV ON INV.PartNo = PN.PartNo
                LEFT JOIN InventoryAlt ALT ON ALT.InventoryIDAlt = INV.InventoryID
                LEFT JOIN Inventory INV2 ON ALT.InventoryID = INV2.InventoryID
                WHERE INV2.MfgID = 1  
                AND Specification = '%s'
                ORDER BY partno, Specification, Description""" % AUTOPLUS_ATTR
    logging.info('Reading specs from autoplus ...')
    mssql_cur.execute(query)
    autoplus_vals = mssql_cur.fetchall()

    qry = """SELECT p.name product_name, l.id as line_id, l.value_id, l.item_specific_attribute_id as atr_id, a.name as atr_name, v.name as val_name
                FROM product_item_specific_line l
                LEFT JOIN product_template p
                ON l.product_tmpl_id = p.id
                LEFT JOIN product_item_specific_attribute a
                ON l.item_specific_attribute_id = a.id
                LEFT JOIN product_item_specific_value as v
                ON l.value_id = v.id
                WHERE a.listing_specific = FALSE
                AND a.name = '%s'""" % attr_id[0]['name']
    cur.execute(qry)
    odoo_vals = cur.fetchall()

    qry = """SELECT distinct SPEC.Description 
                FROM USAPFitment.dbo.Specifications SPEC
                LEFT JOIN USAPFitment.dbo.PartNumbers PN on SPEC.PartID = PN.PartID
                LEFT JOIN Inventory INV on INV.PartNo = PN.PartNo
                LEFT JOIN InventoryAlt ALT on ALT.InventoryIDAlt = INV.InventoryID
                LEFT JOIN Inventory INV2 on ALT.InventoryID = INV2.InventoryID
                WHERE INV2.MfgID = 1  
                AND Specification = '%s'""" % AUTOPLUS_ATTR
    mssql_cur.execute(qry)
    autoplus_unique_vals = mssql_cur.fetchall()

    qry = """SELECT DISTINCT v.name as val_name
                FROM product_item_specific_line l
                LEFT JOIN product_template p
                ON l.product_tmpl_id = p.id
                LEFT JOIN product_item_specific_attribute a
                ON l.item_specific_attribute_id = a.id
                LEFT JOIN product_item_specific_value as v
                ON l.value_id = v.id
                WHERE a.listing_specific = FALSE
                AND a.name = '%s'""" % attr_id[0]['name']
    cur.execute(qry)
    odoo_unique_vals = cur.fetchall()
    odoo_unique_vals_names = [r['val_name'] for r in odoo_unique_vals]

    autoplus_unique_vals_names = []
    for k, v in enumerate(autoplus_unique_vals):
        if v['Description'] not in odoo_unique_vals_names:
            autoplus_unique_vals_names.append(v['Description'])

    # Query to push new values to Odoo
    created_vals = 0
    if len(autoplus_unique_vals_names):
        q_create_vals = """INSERT INTO product_item_specific_value (item_specific_attribute_id, name) VALUES """
        for val in autoplus_unique_vals_names:
            q_create_vals += "(%s,'%s')," % (attr_id[0]['id'], val.replace("'", ""))
            created_vals += 1
        logging.info("Inserting vals ...")
        execute_with_commit(q_create_vals[:-1])

    # Remove lines with products for which line in Odoo already exist
    odoo_products_names = [r['product_name'] for r in odoo_vals]
    autoplus_vals_without_existing = []
    already_has_this_attr = 0
    for k, v in enumerate(autoplus_vals):
        if v['PartNo'] not in odoo_products_names:
            autoplus_vals_without_existing.append(v)
        else:
            already_has_this_attr += 1
    if not len(autoplus_vals_without_existing):
        logging.info('No new lines in autoplus.')
        RUN = False
        return
    # Add product id column to data from autoplus
    qry = """SELECT id, name from product_template where name in ("""
    for r in autoplus_vals_without_existing:
        qry += "'" + r['PartNo'] + "' ,"
    qry = qry[:-1]
    qry += ")"
    cur.execute(qry)
    odoo_products = cur.fetchall()
    for r in autoplus_vals_without_existing:
        for p in odoo_products:
            if r['PartNo'] == p['name']:
                r['id'] = p['id']
                break

    # Add value id column to data from autoplus
    qry = """SELECT v.name as val_name, a.name as atr_name, a.id as atr_id, v.id as val_id
             FROM product_item_specific_value v
             LEFT JOIN product_item_specific_attribute a
             ON v.item_specific_attribute_id = a.id
             WHERE a.name = '%s'""" % attr_id[0]['name']
    cur.execute(qry)
    odoo_vals_ids = cur.fetchall()
    for r in autoplus_vals_without_existing:
        for p in odoo_vals_ids:
            if r['Description'] == p['val_name']:
                r['val_id'] = p['val_id']
                break

    # Request to push new lines to Odoo
    created_lines = 0
    if len(autoplus_vals_without_existing):
        q_create_lines = """INSERT INTO product_item_specific_line (product_tmpl_id, item_specific_attribute_id, value_id) VALUES """
        lines_without_corresponding_records = 0
        for l in autoplus_vals_without_existing:
            if not l.get('id') or not l.get('val_id'):
                lines_without_corresponding_records += 1
                continue
            q_create_lines += """ (%s, %s, %s) ,""" % (l['id'], attr_id[0]['id'], l['val_id'])
            created_lines += 1
        if lines_without_corresponding_records < len(autoplus_vals_without_existing):
            logging.info("Creating lines ...")
            execute_with_commit(q_create_lines[:-1])

    RUN = False
    logging.info("\nStatistic:")
    logging.info("----------")
    logging.info("Initial autoplus lines: %s", len(autoplus_vals))
    logging.info("Odoo lines: %s", len(odoo_vals))
    logging.info("Autoplus unique vals: %s", len(autoplus_unique_vals))
    logging.info("Odoo vals: %s", len(odoo_unique_vals))
    logging.info("New specific values created: %s", created_vals)
    logging.info("Lines that skipped because of already present in odoo: %s", already_has_this_attr)
    logging.info("Autoplus lines intended to push to Odoo: %s", len(autoplus_vals_without_existing))
    logging.info("Autoplus lines that has no match in Odoo by ProductNo: %s", lines_without_corresponding_records)
    logging.info("Created item specific lines in Odoo: %s", created_lines)


def execute_with_commit(q):
    conn = psycopg2.connect(CONN_STR)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(q)
    conn.commit()


def main():
    while RUN:
        logging.info('\n\n\nSTART spec_autoplus_to_odoo.py')
        try:
            autoplus_to_odoo()
        except:
            logging.error('Error: %s' % sys.exc_info()[0])
            logging.error('Error: %s' % sys.exc_info()[1])
            logging.warning('Sleep 3 sec and will continue ...')
            time.sleep(3)
    logging.info('Done !')


if __name__ == "__main__":
    main()

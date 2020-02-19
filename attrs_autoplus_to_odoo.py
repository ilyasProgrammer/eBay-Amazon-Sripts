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
RUN = True
directory = './logs/'
if not os.path.exists(directory):
    os.makedirs(directory)
log_file = directory + '/' + time.strftime("%Y-%m-%d %H:%M:%S") + ' ' + 'spec_autoplus_to_odoo.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S", filemode='a')
logging.getLogger().addHandler(logging.StreamHandler())
attr_model = 'product.auto.attribute'
value_model = 'product.auto.attribute.value'
product_model = 'product.template'
line_model = 'product.auto.attribute.line'
logging.info('Reading all Odoo attrs ...')
attr_ids = rpc.read(attr_model, [['name', '!=', '']])


def autoplus_to_odoo(AUTOPLUS_ATTR):
    global RUN
    conn = psycopg2.connect(CONN_STR)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    logging.info('Searching match to Odoo attrs ...')
    attr_id = False
    for r in attr_ids:
        if r['name'].lower() == AUTOPLUS_ATTR.lower():
            attr_id = r['id']
            break
    if not attr_id:
        execute_with_commit("""INSERT INTO product_auto_attribute (name) VALUES ('%s')""" % AUTOPLUS_ATTR)
        attr_id = rpc.read(attr_model, [['name', '=', AUTOPLUS_ATTR]])[0]['id']

    mssql_conn = pymssql.connect('192.169.152.87', 'UsmanRaja', 'Nop@ss786', 'AutoPlus')

    mssql_cur = mssql_conn.cursor(as_dict=True)
    query = """SELECT  *
               FROM USAPFitment.dbo.Features FEA
               LEFT JOIN USAPFitment.dbo.PartNumbers PN on FEA.PartID = PN.PartID
               LEFT JOIN Inventory INV on INV.PartNo = PN.PartNo
               LEFT JOIN InventoryAlt ALT on ALT.InventoryIDAlt = INV.InventoryID
               LEFT JOIN Inventory INV2 on ALT.InventoryID = INV2.InventoryID
               WHERE INV2.MfgID = 1   
               AND FEA.Feature = '""" + AUTOPLUS_ATTR + "'"
    logging.info('Reading attrs from autoplus ...')
    mssql_cur.execute(query)
    autoplus_vals = mssql_cur.fetchall()

    qry = """SELECT p.name product_name, l.id as line_id, v.id as value_id, l.auto_attribute_id as atr_id, a.name as atr_name, v.name as val_name
             FROM product_auto_attribute_line l
             LEFT JOIN product_template p
             ON l.product_tmpl_id = p.id
             LEFT JOIN product_auto_attribute a
             ON l.auto_attribute_id = a.id
             LEFT JOIN product_auto_attribute_line_product_auto_attribute_value_rel lvr
             ON l.id = lvr.product_auto_attribute_line_id
             LEFT JOIN product_auto_attribute_value as v
             ON lvr.product_auto_attribute_value_id = v.id
             WHERE a.id = '%s'""" % attr_id
    cur.execute(qry)
    odoo_vals = cur.fetchall()

    qry = """SELECT distinct FEA.Description 
             FROM USAPFitment.dbo.Features FEA
             LEFT JOIN USAPFitment.dbo.PartNumbers PN on FEA.PartID = PN.PartID
             LEFT JOIN Inventory INV on INV.PartNo = PN.PartNo
             LEFT JOIN InventoryAlt ALT on ALT.InventoryIDAlt = INV.InventoryID
             LEFT JOIN Inventory INV2 on ALT.InventoryID = INV2.InventoryID
             WHERE INV2.MfgID = 1  
             AND Feature = '%s'""" % AUTOPLUS_ATTR
    mssql_cur.execute(qry)
    autoplus_unique_vals = mssql_cur.fetchall()

    qry = """SELECT DISTINCT v.name as val_name
             FROM product_auto_attribute_line l
             LEFT JOIN product_template p
             ON l.product_tmpl_id = p.id
             JOIN product_auto_attribute a
             ON l.auto_attribute_id = a.id
             LEFT JOIN product_auto_attribute_line_product_auto_attribute_value_rel lvr
             ON l.id = lvr.product_auto_attribute_line_id
             LEFT JOIN product_auto_attribute_value as v
             ON lvr.product_auto_attribute_value_id = v.id
             WHERE a.id = '%s'""" % attr_id
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
        q_create_vals = """INSERT INTO product_auto_attribute_value (auto_attribute_id, name) VALUES """
        for val in autoplus_unique_vals_names:
            q_create_vals += "(%s,'%s')," % (attr_id, val.replace("'", ""))
            logging.info("New value: " + "%s %s" % (AUTOPLUS_ATTR, val.replace("'", "")))
            created_vals += 1
        logging.info("Inserting vals ...")
        execute_with_commit(q_create_vals[:-1])

    # Remove lines with produtcs for which line in Odoo already exist
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
             FROM product_auto_attribute_value v
             LEFT JOIN product_auto_attribute a
             ON v.auto_attribute_id = a.id
             WHERE a.id = %s""" % attr_id
    cur.execute(qry)
    odoo_vals_ids = cur.fetchall()
    for r in autoplus_vals_without_existing:
        for p in odoo_vals_ids:
            if r['Description'] == p['val_name']:
                r['val_id'] = p['val_id']
                break

    # Request to push new lines to Odoo
    created_lines = 0
    lines_without_corresponding_records = 0
    if len(autoplus_vals_without_existing):
        # q_create_rel = """INSERT INTO product_auto_attribute_line_product_auto_attribute_value_rel (product_tmpl_id, auto_attribute_id) VALUES """
        for l in autoplus_vals_without_existing:
            if not l.get('id') or not l.get('val_id'):
                lines_without_corresponding_records += 1
                continue
            res = rpc.create(line_model, {'product_tmpl_id': l['id'], 'auto_attribute_id': attr_id, 'value_ids': [(6, 0, [l['val_id']])]})
            logging.info("New line: %s %s %s %s" % (res, l['Feature'], attr_id, l['Description']))
            created_lines += 1

    # RUN = False
    logging.info("\nStatistic for %s:" % AUTOPLUS_ATTR)
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
    logging.info('\n\n\nSTART spec_autoplus_to_odoo.py')
    try:
        mssql_conn = pymssql.connect('192.169.152.87', 'UsmanRaja', 'Nop@ss786', 'AutoPlus')

        mssql_cur = mssql_conn.cursor(as_dict=True)
        logging.info('Reading DISTINCT Feature from autoplus ...')
        query = """SELECT DISTINCT FEA.Feature
                        FROM USAPFitment.dbo.Features FEA
                        LEFT JOIN USAPFitment.dbo.PartNumbers PN ON FEA.PartID = PN.PartID
                        LEFT JOIN Inventory INV ON INV.PartNo = PN.PartNo
                        LEFT JOIN InventoryAlt ALT ON ALT.InventoryIDAlt = INV.InventoryID
                        LEFT JOIN Inventory INV2 ON ALT.InventoryID = INV2.InventoryID
                        WHERE INV2.MfgID = 1 ORDER BY Feature"""
        mssql_cur.execute(query)
        autoplus_attrs = mssql_cur.fetchall()
        c = 0
        logging.info('Total attrs: %s' % len(autoplus_attrs))
        for attr in autoplus_attrs:
            logging.info('Proceeding feature: %s' % attr['Feature'].lstrip())
            logging.info('\nIter: %s' % c)
            autoplus_to_odoo(attr['Feature'].lstrip())
            c += 1
    except:
        logging.error('Error: %s' % sys.exc_info()[0])
        logging.error('Error: %s' % sys.exc_info()[1])
        logging.warning('Sleep 3 sec and will continue ...')
        time.sleep(3)
    logging.info('Done !')


if __name__ == "__main__":
    main()

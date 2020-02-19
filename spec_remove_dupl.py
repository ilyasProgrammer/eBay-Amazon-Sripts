# # -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import logging
from pprint import pprint, pformat
import sys
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
CONN_STR = "dbname='auto-2016-12-21' user='postgres' host='localhost'"
# CONN_STR = "dbname='auto-2016-12-21' user='auto' host='localhost' password='1q2w3e4r5t!' sslmode=disable"

listing_model = 'product.listing'
store_model = 'sale.store'
attr_model = 'product.item.specific.attribute'
value_model = 'product.item.specific.value'
product_model = 'product.template'
line_model = 'product.item.specific.line'


def go():
    created_lines = 0
    deleted_lines = 0
    deleted_vals = 0
    deleted_attrs = 0
    created_vals = 0

    logging.info("Select attrs that has duplicates ...")
    query = """
            SELECT * FROM 
              (SELECT name, sum(1) sum FROM product_item_specific_attribute GROUP BY  name) z
            WHERE z.sum > 1 """
    conn = psycopg2.connect(CONN_STR)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(query)
    dups = cur.fetchall()

    logging.info("Select unique attrs those who only left ...")
    query = """SELECT DISTINCT ON (NAME) id , NAME
               FROM product_item_specific_attribute where name = 'OEM Number'"""
    cur.execute(query)
    attrs = cur.fetchall()
    dup_names = [r['name'] for r in dups]
    i = 1

    for attr in attrs:
        logging.info(str(i) + '/' + str(len(attrs)))
        i += 1
        if attr['name'] not in dup_names:
            continue
        logging.info("Select aliens values ...")
        query = """SELECT DISTINCT ON (v.name) v.id val_id, v.name val_name, a.id atr_id, a.name atr_name
                   FROM product_item_specific_value v
                   LEFT JOIN product_item_specific_attribute a
                   on v.item_specific_attribute_id = a.id
                   where a.name = '%s' AND a.id != %s""" % (attr['name'], attr['id'])
        cur.execute(query)
        alien_vals = cur.fetchall()

        logging.info("Select main attr values ...")
        query = """SELECT DISTINCT ON (v.name) v.id val_id, v.name val_name, a.id atr_id, a.name atr_name
                   FROM product_item_specific_value v
                   LEFT JOIN product_item_specific_attribute a
                   on v.item_specific_attribute_id = a.id
                   where a.id = %s""" % attr['id']
        cur.execute(query)
        un_atr_vals = cur.fetchall()
        values_of_atr = [r['val_name'] for r in un_atr_vals]
        absent_vals = filter(lambda x: x['val_name'] not in values_of_atr, alien_vals)
        logging.info("Prepare inserts for values ...")
        if absent_vals:
            q_create_vals = """INSERT INTO product_item_specific_value (item_specific_attribute_id, name) VALUES """
            for val in absent_vals:
                q_create_vals += "(%s,'%s')," % (attr['id'], val['val_name'])
                created_vals += 1
            logging.info("Insert vals ...")
            execute_with_commit(q_create_vals[:-1])

        # Read uniq attr vals again with new values just created
        logging.info("Select unique attr values again with new values just created  ...")
        query = """SELECT v.id val_id, v.name val_name, a.id atr_id, a.name atr_name
                   FROM product_item_specific_value v
                   LEFT JOIN product_item_specific_attribute a
                   on v.item_specific_attribute_id = a.id
                   where v.item_specific_attribute_id = %s""" % attr['id']
        cur.execute(query)
        un_atr_vals = cur.fetchall()

        logging.info("Select aliens attrs lines ...")
        query = """SELECT l.id as l_id, l.product_tmpl_id as prod_id, a.id as atr_id,  v.id as val_id, a.name as atr_name, v.name as val_name
                    FROM product_item_specific_line l
                      LEFT JOIN product_item_specific_attribute a
                        ON l.item_specific_attribute_id = a.id
                      LEFT JOIN product_item_specific_value v
                        ON l.value_id = v.id
                    WHERE a.name = '%s' AND a.id != %s""" % (attr['name'], attr['id'])
        cur.execute(query)
        alien_lines = cur.fetchall()

        logging.info("Select main attr lines ...")
        query = """SELECT l.id as l_id, l.product_tmpl_id as prod_id, a.id as atr_id,  v.id as val_id, a.name as atr_name, v.name as val_name
                    FROM product_item_specific_line l
                      LEFT JOIN product_item_specific_attribute a
                        ON l.item_specific_attribute_id = a.id
                      LEFT JOIN product_item_specific_value v
                        ON l.value_id = v.id
                    WHERE a.name = '%s' AND a.id = %s""" % (attr['name'], attr['id'])
        cur.execute(query)
        own_lines = cur.fetchall()
        lines_to_delete = []
        vals_to_delete = []
        lines_to_create = []
        tot = len(alien_lines)
        it = 1
        ex = 0
        un = 0
        logging.info("Find lines to keep and lines to delete ...")
        for al in alien_lines:
            if not al['val_name']:
                continue
            # logging.info("Step: %s/%s", it, tot)
            it += 1
            has = False
            for ol in own_lines:
                if al['prod_id'] == ol['prod_id'] and al['val_name'] == ol['val_name']:
                    ex += 1
                    # logging.info("Found existing line. Total found: %s", ex)
                    has = True
                    break
            lines_to_delete.append(al['l_id'])
            # logging.info("Line to delete: %s", al['l_id'])
            vals_to_delete.append(al['val_id'])
            # logging.info("Val to delete: %s", al['val_id'])
            if not has:
                un += 1
                # logging.info("Match absent. Total unmatched: %s", un)
                val_id = find_val_id(un_atr_vals, al['val_name'])
                lines_to_create.append({'prod_id': al['prod_id'], 'val_id': val_id, 'atr_id': attr['id']})

        # Create lines
        if len(lines_to_create):
            logging.info("Creating lines ...")
            q_create_lines = """INSERT INTO product_item_specific_line (product_tmpl_id, item_specific_attribute_id, value_id) VALUES """
            for l in lines_to_create:
                q_create_lines += """ (%s, %s, %s) ,""" % (l['prod_id'], l['atr_id'], l['val_id'])
            execute_with_commit(q_create_lines[:-1])
            created_lines = len(lines_to_create)

        # Delete vals
        if len(vals_to_delete):
            q_delete_vals = """DELETE FROM product_item_specific_value WHERE id IN """
            ids = ''
            limit = 0
            for ind, l in enumerate(vals_to_delete):
                ids += "%s," % l
                limit += 1
                if limit == 500 or ind+1 == len(vals_to_delete):
                    logging.info("Deleting vals ...")
                    execute_with_commit(q_delete_vals + "(" + ids[:-1] + ")")
                    ids = ''
                    limit = 0
            deleted_vals = len(vals_to_delete)

        # Delete lines
        if len(lines_to_delete):
            q_delete_lines = """DELETE FROM product_item_specific_line WHERE id in """
            ids = ''
            limit = 0
            for ind,l in enumerate(lines_to_delete):
                ids += "%s ," % l
                limit += 1
                if limit == 500 or ind+1 == len(lines_to_delete):
                    logging.info("Deleting lines ...")
                    execute_with_commit(q_delete_lines + "(" + ids[:-1] + ")")
                    ids = ''
                    limit = 0
            deleted_lines = len(lines_to_delete)

        # Delete all aliens attrs
        query = """SELECT id 
                       FROM product_item_specific_attribute where name = '%s' AND id != %s""" % (attr['name'], attr['id'])
        cur.execute(query)
        attrs_to_delete = cur.fetchall()
        # attrs_to_delete = list(set([r['atr_id'] for r in alien_lines]))
        if len(attrs_to_delete):
            q_delete_attrs = """DELETE FROM product_item_specific_attribute WHERE id IN """
            ids = ''
            limit = 0
            for ind,attr_id in enumerate(attrs_to_delete):
                ids += "%s," % attr_id['id']
                limit += 1
                if limit == 10 or ind+1 == len(attrs_to_delete):
                    logging.info("Deleting attrs ...")
                    execute_with_commit(q_delete_attrs + "(" + ids[:-1] + ")")
                    ids = ''
                    limit = 0
            deleted_attrs = len(attrs_to_delete)

    logging.info("created_lines: %s", created_lines)
    logging.info("created_vals: %s", created_vals)
    logging.info("deleted_lines: %s", deleted_lines)
    logging.info("deleted_attrs: %s", deleted_attrs)
    logging.info("deleted_vals: %s", deleted_vals)
    logging.info("DONE")


def execute_with_commit(q):
    conn = psycopg2.connect(CONN_STR)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(q)
    conn.commit()


def find_val_id(un_atr_vals, name):
    for r in un_atr_vals:
        if name == r['val_name']:
            return r['val_id']
    return 1/0


def estimate_qry(q):
    return
    conn = psycopg2.connect(CONN_STR)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("EXPLAIN ANALYZE VERBOSE " + q)
    logging.info("Estimation for query: \n %s", pformat(cur.fetchall()))


def main():
    logging.info('\n\n\nSTART spec_sync_odoo_ebay.py')
    try:
        go()
    except:
        logging.error('Error: %s' % sys.exc_info()[0])
        logging.error('Error: %s' % sys.exc_info()[1])


if __name__ == "__main__":
    main()

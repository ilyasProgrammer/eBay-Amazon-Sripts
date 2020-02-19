import pymssql
import logging
import linecache
import xmlrpclib
import psycopg2
import psycopg2.extras

url = 'http://opsyst.com'
db = 'auto-2016-12-21'
username = 'admin'
password = 'yohoooo'

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

def get_all_ase_skus_in_odoo():
    conn = psycopg2.connect("dbname='auto-2016-12-21' user='auto' host='45.55.56.99' password='1q2w3e4r5t!'")
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    query = """
        SELECT part_number from product_template
        WHERE mfg_code = 'ASE' ORDER BY inventory_id asc
    """
    cur.execute(query)
    results = cur.fetchall()
    outfile = open('./src/odoo_ase_skus.csv', 'wb')

    for res in results:
        outfile.write('%s\n' %(res['part_number']))
    outfile.close()

def get_all_ase_skus_in_autoplus():
    conn = pymssql.connect('192.169.152.87', 'UsmanRaja', 'Nop@ss786', 'AutoPlus')
    cur = conn.cursor(as_dict=True)
    query = """
        SELECT INV.InventoryId, INV.PartNo FROM Inventory INV
        WHERE INV.MfgId = 1
        ORDER BY INV.InventoryID ASC
    """
    cur.execute(query)
    results = cur.fetchall()
    outfile = open('./src/autoplus_ase_skus.csv', 'wb')

    for res in results:
        outfile.write('%s,%s\n' %(res['InventoryId'], res['PartNo']))
    outfile.close()

def get_inventory_ids_to_sync():
    outfile = open('./src/inv_ids_to_sync.csv', 'wb')
    counter = 1
    for a_line in open('./src/autoplus_ase_skus.csv'):
        print counter
        counter += 1
        for o_line in open('./src/odoo_ase_skus.csv'):
            if a_line.split(',')[1][:-1] == o_line[:-1]:
                break
        else:
            outfile.write('%s\n' %a_line.split(',')[0])
    outfile.close()


def sync_products(searchfile, from_line, to_line):
    models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))
    counter = from_line
    while counter <= to_line:
        line = linecache.getline(searchfile, counter)
        inv_id = int(line[:-1])

        product_tmpl_id = models.execute_kw(db, 1, password, 'product.template', 'search',
            [[['inventory_id', '=', inv_id ]]])

        if product_tmpl_id:
            models.execute_kw(db, 1, password, 'product.template', 'button_sync_with_autoplus', [[product_tmpl_id[0]]], {'raise_exception': False})
            logging.info('%s: Updated %s' %(counter, inv_id))
        else:
            new_product = models.execute_kw(db, 1, password, 'product.template', 'create', [{'name': 'New', 'inventory_id': inv_id}])
            models.execute_kw(db, 1, password, 'product.template', 'button_sync_with_autoplus', [[new_product]], {'raise_exception': False})
            logging.info('%s: Created %s' %(counter, inv_id))

        counter += 1
    logging.info('Done processing.')

def sync_products_by_lad_sku(searchfile, from_line, to_line):
    conn = pymssql.connect('192.169.152.87', 'UsmanRaja', 'Nop@ss786', 'AutoPlus')
    cur = conn.cursor(as_dict=True)
    models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))
    counter = from_line
    not_in_autoplus = []
    while counter <= to_line:
        line = linecache.getline(searchfile, counter)
        sku = line[:-1]

        query = """
            SELECT INV.InventoryID FROM Inventory INV
            WHERE INV.PartNo = '%s'
        """ %sku
        cur.execute(query)
        results = cur.fetchall()

        if not results:
            logging.info('%s: %s is not in AutoPlus' %(counter, sku))
            not_in_autoplus.append(sku)
            counter += 1
            continue

        inv_id = results[0]['InventoryID']

        product_tmpl_id = models.execute_kw(db, 1, password, 'product.template', 'search',
            [[['inventory_id', '=', inv_id ]]])

        if product_tmpl_id:
            models.execute_kw(db, 1, password, 'product.template', 'button_sync_with_autoplus', [[product_tmpl_id[0]]], {'raise_exception': False})
            logging.info('%s: Updated %s' %(counter, sku))
        else:
            new_product = models.execute_kw(db, 1, password, 'product.template', 'create', [{'name': 'New', 'inventory_id': inv_id}])
            models.execute_kw(db, 1, password, 'product.template', 'button_sync_with_autoplus', [[new_product]], {'raise_exception': False})
            logging.info('%s: Created %s' %(counter, sku))

        counter += 1
    logging.info('NOT IN AUTOPLUS %s' %not_in_autoplus)
    logging.info('Done processing.')


def main():
    get_all_ase_skus_in_odoo()
    get_all_ase_skus_in_autoplus()
    get_inventory_ids_to_sync()
    sync_products('/Users/ajporlante/auto/products/.csv', 353, 1000)
    sync_products_by_lad_sku('/Users/ajporlante/Downloads/feap.csv', 501, 504)
    pass


if __name__ == "__main__":
    main()
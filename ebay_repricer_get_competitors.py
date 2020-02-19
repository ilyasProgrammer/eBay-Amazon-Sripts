import argparse
import json
import logging
import os
import pymssql
import psycopg2
import psycopg2.extras
import simplejson
import sys

from datetime import datetime, timedelta
import ebaysdk
from ebaysdk.exception import ConnectionError
from ebaysdk.shopping import Connection as Shopping
from ebaysdk.trading import Connection as Trading
test_only = False

if '--test-only' in sys.argv:
    test_only = True
parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('--offset', action="store", dest="offset")
args = vars(parser.parse_args())
offset = int(args['offset']) if args['offset'] else 0

thisdir = os.path.abspath(os.path.dirname( __file__ ))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', filename='/mnt/vol/log/cron/repricer.log', datefmt='%Y-%m-%d %H:%M:%S')
# logging.basicConfig(level=logging.INFO)

odoo_url = config.get('odoo_url')
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
odoo_uid = config.get('odoo_uid')
odoo_password = config.get('odoo_password')
stores = config.get('stores').split(',')
ebay_app_id = config.get('ebay_app_id')

def odoo_execute(query, commit=False):
    conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'"
                %(odoo_db, odoo_db_user, odoo_db_host, odoo_db_password)
            )
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute(query)
    if commit:
        conn.commit()
        return True
    else:
        results = cur.fetchall()
        return results

def autoplus_execute(query):
    conn = pymssql.connect('192.169.152.87', 'UsmanRaja', 'Nop@ss786', 'AutoPlus')
    cur = conn.cursor(as_dict=True)
    cur.execute(query)
    results = cur.fetchall()
    return results

def get_store_credentials():
    credentials = {}
    stores_for_query = str(stores).replace('[', '(').replace(']', ')')
    results = odoo_execute("SELECT * FROM sale_store WHERE code in %s" %stores_for_query)
    for res in results:
        credential = {
            'name': res['name'],
            'type': res['site'],
            'id': res['id']
        }
        if res['site'] == 'ebay':
            credential['max_qty'] = res['ebay_max_quantity']
            credential['rate'] = float(res['ebay_rate'])
            credential['last_record_number']= res['ebay_last_record_number']
            credential['domain'] = res['ebay_domain']
            credential['developer_key'] = res['ebay_dev_id']
            credential['application_key'] = res['ebay_app_id']
            credential['certificate_key'] = res['ebay_cert_id']
            credential['auth_token'] = res['ebay_token']

            credentials[res['code']] = credential
    return credentials

def get_wh_cost(part_number):

    SHIP_PICK_ID = 4
    create_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')

    wh_cost = odoo_execute("""
        SELECT (CASE WHEN BASE_COST.cost > 0 THEN BASE_COST.cost ELSE 0 END) as wh_cost, (CASE WHEN SHIPPING.rate > 0 THEN SHIPPING.rate ELSE 0 END) as wh_shipping_cost
        FROM (
            SELECT QUANT.product_id, SUM(QUANT.qty) as qty, SUM(QUANT.qty * QUANT.cost) / SUM(QUANT.qty) as cost
            FROM stock_quant QUANT
            LEFT JOIN stock_location LOC on QUANT.location_id = LOC.id
            WHERE LOC.usage = 'internal' AND QUANT.cost > 0 AND QUANT.qty > 0 AND QUANT.reservation_id IS NULL
            GROUP BY QUANT.product_id
        ) as BASE_COST
        LEFT JOIN (
            SELECT MOVE.product_id, (CASE WHEN SUM(MOVE.product_uom_qty) > 0 THEN SUM(PICKING.rate)/SUM(MOVE.product_uom_qty) ELSE 0 END) as rate
            FROM stock_move MOVE
            LEFT JOIN stock_picking PICKING ON PICKING.id = MOVE.picking_id
            WHERE MOVE.create_date >= '%s' AND PICKING.picking_type_id = %s AND PICKING.state = 'done' AND PICKING.id NOT IN (
                    SELECT PICKING2.id FROM stock_picking PICKING2
                    LEFT JOIN stock_move MOVE2 on MOVE2.picking_id = PICKING2.id
                    GROUP BY PICKING2.id HAVING COUNT(*) > 1
                )
            GROUP BY MOVE.product_id
        ) as SHIPPING on SHIPPING.product_id = BASE_COST.product_id
        LEFT JOIN product_product PRODUCT on PRODUCT.id = BASE_COST.product_id
        LEFT JOIN product_template TEMPLATE on TEMPLATE.id = PRODUCT.product_tmpl_id
        WHERE TEMPLATE.part_number = '%s'
    """ %(create_date, SHIP_PICK_ID, part_number))

    total_cost = 0.0
    if wh_cost:
        wh_cost = wh_cost[0]
        total_cost += wh_cost['wh_cost']
        if wh_cost['wh_shipping_cost'] > 0:
            total_cost += wh_cost['wh_shipping_cost']
        else:
            pfg_shipping_cost = autoplus_execute("""
                SELECT
                (USAP.ShippingPrice + USAP.HandlingPrice) as ShippingPrice
                FROM Inventory INV
                LEFT JOIN InventoryAlt ALT on ALT.InventoryID = INV.InventoryID
                LEFT JOIN Inventory INV2 on ALT.InventoryIDAlt = INV2.InventoryID
                LEFT JOIN USAP.dbo.Warehouse USAP ON INV2.PartNo = USAP.PartNo
                WHERE INV2.MfgID IN (16,35,36,37,38,39) AND USAP.ShippingPrice > 0 AND INV.PartNo = '%s'
            """ %part_number)
            if pfg_shipping_cost and pfg_shipping_cost[0]['ShippingPrice'] > 0:
                total_cost += float(pfg_shipping_cost[0]['ShippingPrice'])
            else:
                total_cost *= 1.1
    return total_cost

def get_vendor_cost(part_number):
    VENDOR_MFG_LABELS = {
        'LKQ': { 'mfg_labels': "('BXLD', 'GMK')", 'less_qty': 5 },
        'TAW': { 'mfg_labels': "('S/B')", 'less_qty': 5},
        'PFG': { 'mfg_labels': "('BFHZ', 'REPL', 'STYL', 'NDRE', 'BOLT', 'EVFI')", 'less_qty': 5 }
    }
    subquery = ''
    vendor_counter = 1
    for vendor in VENDOR_MFG_LABELS:
        subquery += """
            (SELECT INV2.PartNo as part_number, PR.Cost as cost
            FROM InventoryAlt ALT
            LEFT JOIN Inventory INV on ALT.InventoryIDAlt = INV.InventoryID
            LEFT JOIN Inventory INV2 on ALT.InventoryID = INV2.InventoryID
            LEFT JOIN InventoryMiscPrCur PR ON INV.InventoryID = PR.InventoryID
            LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
            WHERE MFG.MfgCode IN %s AND INV.QtyOnHand > %s AND INV2.PartNo = '%s')

            UNION

            (SELECT INV.PartNo as part_number, PR.Cost as cost
            FROM Inventory INV
            LEFT JOIN InventoryMiscPrCur PR ON INV.InventoryID = PR.InventoryID
            LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
            WHERE MFG.MfgCode IN %s AND INV.QtyOnHand > %s AND INV.PartNo = '%s')
        """ %(VENDOR_MFG_LABELS[vendor]['mfg_labels'], VENDOR_MFG_LABELS[vendor]['less_qty'], part_number,
              VENDOR_MFG_LABELS[vendor]['mfg_labels'], VENDOR_MFG_LABELS[vendor]['less_qty'], part_number)
        if vendor_counter < len(VENDOR_MFG_LABELS):
            subquery += 'UNION'
        vendor_counter += 1

    res = autoplus_execute("""
        SELECT RES.part_number, MIN(RES.cost) as cost FROM
            (
                %s
            ) as RES GROUP BY RES.part_number
        """ % subquery
        )

    return float(res[0]['cost']) if res and res[0]['cost'] else 0.0

def get_competitors(ebay_app_id, credentials):
    OWNED_STORES = ['reviveautopart', 'visionaryautoparts', 'rideautosupply', 'rhinorexauto']
    RATES = {
        'visionary': 1.21,
        'visionary_mg': 1.20,
        'revive': 1.22,
        'revive_apc': 1.225,
        'rhino': 1.23,
        'ride': 1.24
    }
    total_skus_with_competitors = autoplus_execute("""
        SELECT COUNT(*) as Count FROM eBayRepricer.dbo.Repricer
    """)

    if not total_skus_with_competitors:
        logging.info('No competitor found from AutoPlus database.')
        return

    total_skus_with_competitors = total_skus_with_competitors[0]['Count']
    max_counter = min(offset + 15000, total_skus_with_competitors)

    counter = offset + 1
    ebay_shopping_api = Shopping(config_file=None, appid=ebay_app_id, siteid='100')

    while counter < max_counter:
        skus = autoplus_execute("""SELECT InventoryID, eBayItemIDs FROM
            eBayRepricer.dbo.Repricer
            ORDER BY InventoryID
            OFFSET %s ROWS
            FETCH NEXT %s ROWS ONLY
        """ %(counter - 1, 100))

        for sku in skus:
            create_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if sku['eBayItemIDs']:
                item_ids = sku['eBayItemIDs'].split(',')
                batches = len(item_ids) / 20
                competitor_prices = []
                batch = 0
                while batch <= batches:
                    batch_item_ids = item_ids[batch * 20: min(len(item_ids), (batch + 1) * 20)]
                    try:
                        result = ebay_shopping_api.execute('GetMultipleItems', {'ItemID': batch_item_ids, 'IncludeSelector': 'Details'}).dict()
                        if 'Item' in result:
                            items = result['Item']
                            if not isinstance(items, list):
                                items = [items]
                            for res in items:
                                if res['Seller']['UserID'] not in OWNED_STORES and (res["Quantity"]) > 0:
                                    competitor_prices.append(float(res['ConvertedCurrentPrice']['value']))
                    except:
                        logging.error('Something went wrong with the API call')
                    batch += 1

                lad_skus = autoplus_execute("""SELECT INV.PartNo FROM Inventory INV
                    LEFT JOIN InventoryAlt ALT on ALT.InventoryId = INV.InventoryId
                    WHERE ALT.InventoryIdAlt = %s AND INV.MfgId = 1
                """ %sku['InventoryID'])

                if not lad_skus:
                    logging.error('No LAD SKU found for %s' %sku)
                    continue

                for lad_sku in lad_skus:

                    has_update = False
                    part_number = lad_sku['PartNo']

                    if part_number in ['02-0903-01313','02-0903-01315']:
                        continue

                    product_tmpl_id = odoo_execute("""
                        SELECT PT.id, BOM.id as bom_id FROM product_template PT
                        LEFT JOIN mrp_bom BOM on BOM.product_tmpl_id = PT.id
                        WHERE PT.part_number = '%s'
                        LIMIT 1
                    """ %part_number)

                    if not product_tmpl_id:
                        logging.error('No product found in Opsyst for %s' %sku)
                        continue

                    if product_tmpl_id[0]['bom_id']:
                        logging.error('Product found in Opsyst for %s is a kit' %sku)
                        continue

                    product_tmpl_id = product_tmpl_id[0]['id']
                    current_listings = odoo_execute("""
                        SELECT LISTING.id, LISTING.name, STORE.code, LISTING.current_price,
                        (CASE WHEN LISTING.custom_label IS NOT NULL THEN LISTING.custom_label ELSE 'no_label' END) as custom_label
                        FROM product_listing LISTING
                        LEFT JOIN sale_store STORE on STORE.id = LISTING.store_id
                        WHERE LISTING.product_tmpl_id = %s AND STORE.site = 'ebay' AND LISTING.state = 'active'
                        AND STORE.code IN ('visionary', 'revive', 'ride', 'rhino')
                        AND (LISTING.custom_label NOT LIKE 'X%%' OR LISTING.custom_label IS NULL)
                    """ %product_tmpl_id)

                    if not current_listings:
                        logging.error('No listings found in Opsyst for %s' %sku)
                        continue

                    wh_cost = get_wh_cost(part_number)
                    vendor_cost = get_vendor_cost(part_number)

                    cost = 0
                    if wh_cost > 0 or vendor_cost > 0:
                        cost = min(cost for cost in [wh_cost, vendor_cost] if cost > 0)

                    if cost > 0:
                        lowest_price = 1.3 * cost
                        if competitor_prices:
                            own_lowest_price = min(l['current_price'] for l in current_listings)
                            lowest_competitor_price = min(competitor_prices)
                            if own_lowest_price >= lowest_competitor_price:
                                lowest_price = max(lowest_competitor_price - 0.10, 1.13 * cost)

                        current_listings = sorted(current_listings, key=lambda l: l['current_price'])
                        lowest_listing = current_listings[0]

                        lowest_rate = RATES[lowest_listing['code']]
                        if lowest_listing['custom_label'].startswith('APC'):
                            lowest_rate = RATES['revive_apc']
                        elif lowest_listing['custom_label'].startswith('MG'):
                            lowest_rate = RATES['visionary_mg']

                        base_rate = (lowest_price - cost) / cost

                        for listing in current_listings:
                            rate = RATES[listing['code']]
                            if listing['custom_label'].startswith('APC'):
                                rate = RATES['revive_apc']
                            elif listing['custom_label'].startswith('MG'):
                                lowest_rate = RATES['visionary_mg']
                            new_price = float("{0:.2f}".format((1.0 + base_rate + (rate - lowest_rate)) * cost))

                            if int(listing['current_price'] * 100) != int(new_price * 100):
                                has_update = True
                                action = 'increase'
                                if listing['current_price'] > new_price:
                                    action = 'decrease'

                                logging.info('%s for %s will be repriced from %s to %s.' %(listing['name'], part_number, listing['current_price'], new_price))
                                # Update store
                                item_dict = { 'ItemID': listing['name'], 'StartPrice': new_price }
                                cred = credentials[listing['code']]
                                ebay_trading_api = Trading(domain='api.ebay.com',
                                   config_file=None,
                                   appid=cred['application_key'],
                                   devid=cred['developer_key'],
                                   certid=cred['certificate_key'],
                                   token=cred['auth_token'],
                                   siteid='100'
                                )
                                try:
                                    ebay_trading_api.execute('ReviseItem', {'Item': item_dict})
                                except:
                                     logging.error('Revising item %s for %s failed' %(listing['name'], listing['code']))

                                percent = 0.0
                                try:
                                    percent = abs (listing['current_price'] - new_price) / listing['current_price']
                                except:
                                    logging.error('Current price is set to 0')

                                # Create a new update record
                                odoo_execute("""
                                    INSERT INTO repricer_update_line (product_listing_id, action, percent, old_price, new_price, create_date ) VALUES
                                    (%s, '%s', %s, %s, %s, '%s')
                                """ %(listing['id'], action, percent, listing['current_price'], new_price, create_date), commit=True)

                                # Update current price of listing
                                odoo_execute("""
                                    UPDATE product_listing SET current_price = %s WHERE id = %s
                                """ %(new_price, listing['id']), commit=True)

                    # Update product to use eBay repricer
                    if has_update:
                        odoo_execute("""
                            UPDATE product_template SET use_ebay_repricer = true WHERE part_number = '%s'
                        """ %(part_number), commit=True)
        counter += 100
      	logging.info('Processed %s SKUS' %counter)

def main():
    credentials = get_store_credentials()

    ebay_app_id = ''
    for c in credentials:
        if c in ['visionary', 'ride', 'revive', 'rhino']:
            ebay_app_id = credentials[c]['application_key']
            break

    get_competitors(ebay_app_id, credentials)

if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-

"""
Update Vendor cost and availability of products
"""

import argparse
import logging
import os
import simplejson
import opsystConnection
from datetime import datetime, timedelta
import getpass
import sys
import slack

logging.captureWarnings(True)
logging.getLogger('requests').setLevel(logging.ERROR)
current_user = getpass.getuser()
this_module_name = os.path.basename(sys.modules['__main__'].__file__)
parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-test-only', action="store", dest="test_only")
parser.add_argument('-update-all', action="store", dest="update_all")
args = vars(parser.parse_args())
test_only = bool(args['test_only'])
if args['test_only'] in ('0', 'False'):
    test_only = False
update_all = bool(args['update_all'])
if args['update_all'] in ('0', 'False'):
    update_all = False

thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
stores = config.get('stores').split(',')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')

LOG_FILE = '/mnt/vol/log/cron/vendor_stocks_%s.log' % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)

Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)

SYNC_HOURS = 2
DT_FORMAT = '%Y-%m-%d %H:%M:%S'


def update_vendor_qty():
    # Get last update timestamp for qtys
    now = datetime.utcnow()
    if update_all:
        now = datetime.strptime('2000-01-01 00:00:00', DT_FORMAT)

    last_qty_write_date = '2000-01-01 00:00:00'
    last_qty_write_date_res = Connection.odoo_execute("""
        SELECT vendor_qty_write_date FROM product_template_listed
        WHERE state = 'active' AND vendor_qty_write_date IS NOT NULL
        ORDER BY vendor_qty_write_date DESC
        LIMIT 1
    """)

    if last_qty_write_date_res and last_qty_write_date_res[0]['vendor_qty_write_date']:
        last_qty_write_date = last_qty_write_date_res[0]['vendor_qty_write_date'].strftime(DT_FORMAT)
        hours_ago = (now - timedelta(hours=SYNC_HOURS)).strftime(DT_FORMAT)
        last_qty_write_date = min(last_qty_write_date, hours_ago)

    logging.info("Updating vendor qty of singles")

    qty_res = Connection.autoplus_execute("""
        SELECT INV2.PartNo, SUM(CASE WHEN QTYDM.QtyCur > 5 THEN (QTYDM.QtyCur - 5) ELSE 0 END) as qty
        FROM InventoryAlt ALT
        LEFT JOIN Inventory INV on ALT.InventoryIDAlt = INV.InventoryID
        LEFT JOIN Inventory INV2 on ALT.InventoryID = INV2.InventoryID
        LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
        LEFT JOIN AutoPlus.dbo.MonitorQtyChange QTYDM ON QTYDM.InventoryID = INV.InventoryID
        WHERE MFG.MfgCode IN ('BXLD', 'GMK', 'BFHZ', 'REPL', 'STYL', 'NDRE', 'BOLT', 'EVFI', 'S/B')
        AND INV2.InventoryID IN (
            SELECT INV2.InventoryID
            FROM InventoryAlt ALT
            LEFT JOIN Inventory INV on ALT.InventoryIDAlt = INV.InventoryID
            LEFT JOIN Inventory INV2 on ALT.InventoryID = INV2.InventoryID
            LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
            LEFT JOIN AutoPlus.dbo.MonitorQtyChange QTYDM ON QTYDM.InventoryID = INV.InventoryID
            WHERE MFG.MfgCode IN ('BXLD', 'GMK', 'BFHZ', 'REPL', 'STYL', 'NDRE', 'BOLT', 'EVFI', 'S/B')
            AND INV2.MfgID = 1 AND QTYDM.DateModified >= %s
        )
        GROUP BY INV2.PartNo
    """, [last_qty_write_date])

    if qty_res:
        qty_res_len = len(qty_res)
        batches = qty_res_len / 1000
        if qty_res_len % 1000 == 0:
            batches -= 1

        logging.info("To update vendor qty of %s listed products in %s batches" %(qty_res_len, batches))
        batch = 0
        while batch <= batches:
            logging.info("Processing batch %s." %(batch))
            batch_list = qty_res[batch * 1000: min(qty_res_len, (batch + 1) * 1000)]

            part_numbers = [p['PartNo'] for p in batch_list]
            mapping = {}
            product_tmpl_id_list = []
            mapping_res = Connection.odoo_execute("""
                SELECT id, part_number FROM product_template WHERE part_number IN %s
            """, [part_numbers])
            for m in mapping_res:
                mapping[m['part_number']] = m['id']
                product_tmpl_id_list.append(m['id'])

            vendor_qty_cases = ''
            for p in batch_list:
                if p['PartNo'] in mapping:
                    vendor_qty_cases += "WHEN product_tmpl_id=%s THEN %s " %(mapping[p['PartNo']], p['qty'])
                else:
                    logging.error("Product not saved in Opsyst %s" %p['PartNo'])
            if vendor_qty_cases:
                query_1 = "UPDATE product_template_listed SET vendor_qty = (CASE %s END), " %vendor_qty_cases[:-1]
                query_2 = "vendor_qty_write_date = now() at time zone 'UTC' WHERE product_tmpl_id IN %s"
                Connection.odoo_execute(query_1 +  query_2, [product_tmpl_id_list], commit=True)
            batch += 1

    # Update qtys of kits whose components are in qty_res
    logging.info("Updating vendor qty of kits")
    Connection.odoo_execute("""
        UPDATE product_template_listed
        SET
            vendor_qty = RES.vendor_qty,
            vendor_qty_write_date = now() at time zone 'UTC'
        FROM (
            SELECT
                PTL.id,
                (CASE WHEN MIN(CASE WHEN PTLCOMP.vendor_qty IS NULL THEN 0 ELSE PTLCOMP.vendor_qty END) > 0 THEN MIN(PTLCOMP.vendor_qty) ELSE 0 END) as vendor_qty
            FROM product_template_listed PTL
            LEFT JOIN mrp_bom BOM ON BOM.product_tmpl_id = PTL.product_tmpl_id
            LEFT JOIN mrp_bom_line BOMLINE ON BOM.id = BOMLINE.bom_id
            LEFT JOIN product_product PP on PP.id = BOMLINE.product_id
            LEFT JOIN product_template PT on PP.product_tmpl_id = PT.id
            LEFT JOIN product_template_listed PTLCOMP ON PTLCOMP.product_tmpl_id = PT.id
            WHERE PTL.id IN (
                SELECT PTLKIT.id FROM product_template_listed PTLCOMP
                LEFT JOIN product_product PPCOMP ON PPCOMP.product_tmpl_id = PTLCOMP.product_tmpl_id
                LEFT JOIN mrp_bom_line BOMLINE ON BOMLINE.product_id = PPCOMP.id
                LEFT JOIN mrp_bom BOM ON BOM.id = BOMLINE.bom_id
                LEFT JOIN product_template_listed PTLKIT ON PTLKIT.product_tmpl_id = BOM.product_tmpl_id
                WHERE PTLKIT.product_tmpl_id IS NOT NULL AND PTLCOMP.vendor_qty_write_date >= %s
            )
            GROUP BY PTL.id
        ) as RES WHERE RES.id = product_template_listed.id
    """, [last_qty_write_date], commit = True)


def update_vendor_cost():
    # Get last update timestamp for qtys

    now = datetime.utcnow()
    if update_all:
        now = datetime.strptime('2000-01-01 00:00:00', DT_FORMAT)

    last_vendor_cost_write_date = '2000-01-01 00:00:00'
    last_vendor_cost_write_date_res = Connection.odoo_execute("""
        SELECT vendor_cost_write_date FROM product_template_listed
        WHERE state = 'active' AND vendor_cost_write_date IS NOT NULL
        ORDER BY vendor_cost_write_date DESC
        LIMIT 1
    """)
    if last_vendor_cost_write_date_res and last_vendor_cost_write_date_res[0]['vendor_cost_write_date']:
        last_vendor_cost_write_date = last_vendor_cost_write_date_res[0]['vendor_cost_write_date'].strftime(DT_FORMAT)
        hours_ago = (now - timedelta(hours=SYNC_HOURS)).strftime(DT_FORMAT)
        last_vendor_cost_write_date = min(last_vendor_cost_write_date, hours_ago)

    logging.info("Updating vendor cost of singles")

    vendor_cost_res = Connection.autoplus_execute("""
        SELECT INV2.PartNo,
        (CASE WHEN (MIN(CASE WHEN COSTDM.CostCur = 0 THEN 10000 ELSE COSTDM.CostCur END)) = 10000 THEN 0 ELSE MIN(CASE WHEN COSTDM.CostCur = 0 THEN 10000 ELSE COSTDM.CostCur END) END) as cost
        FROM InventoryAlt ALT
        LEFT JOIN Inventory INV on ALT.InventoryIDAlt = INV.InventoryID
        LEFT JOIN Inventory INV2 on ALT.InventoryID = INV2.InventoryID
        LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
        LEFT JOIN AutoPlus.dbo.MonitorCostChange COSTDM ON COSTDM.InventoryID = INV.InventoryID
        WHERE MFG.MfgCode IN ('BXLD', 'GMK', 'BFHZ', 'REPL', 'STYL', 'NDRE', 'BOLT', 'EVFI', 'S/B')
        AND INV.QtyOnHand > 5
        AND INV2.InventoryID IN (
            SELECT INV2.InventoryID
            FROM InventoryAlt ALT
            LEFT JOIN Inventory INV on ALT.InventoryIDAlt = INV.InventoryID
            LEFT JOIN Inventory INV2 on ALT.InventoryID = INV2.InventoryID
            LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
            LEFT JOIN AutoPlus.dbo.MonitorCostChange COSTDM ON COSTDM.InventoryID = INV.InventoryID
            WHERE MFG.MfgCode IN ('BXLD', 'GMK', 'BFHZ', 'REPL', 'STYL', 'NDRE', 'BOLT', 'EVFI', 'S/B')
            AND INV2.MfgID = 1
        )
        GROUP BY INV2.PartNo
    """)

    if vendor_cost_res:
        vendor_cost_res_len = len(vendor_cost_res)
        batches = vendor_cost_res_len / 1000
        if vendor_cost_res_len % 1000 == 0:
            batches -= 1

        logging.info("To update vendor cost of %s listed products in %s batches" %(vendor_cost_res_len, batches))
        batch = 0
        while batch <= batches:
            logging.info("Processing batch %s." %(batch))
            batch_list = vendor_cost_res[batch * 1000: min(vendor_cost_res_len, (batch + 1) * 1000)]

            part_numbers = [p['PartNo'] for p in batch_list]
            mapping = {}
            product_tmpl_id_list = []
            mapping_res = Connection.odoo_execute("""
                SELECT id, part_number FROM product_template WHERE part_number IN %s
            """, [part_numbers])
            for m in mapping_res:
                mapping[m['part_number']] = m['id']
                product_tmpl_id_list.append(m['id'])

            vendor_cost_cases = ''
            for p in batch_list:
                if p['PartNo'] in mapping and p['cost'] > 0:
                    vendor_cost_cases += "WHEN product_tmpl_id=%s THEN %s " %(mapping[p['PartNo']], float(p['cost']))
                elif p['PartNo'] not in mapping:
                    logging.error("Product not saved in Opsyst %s" %p['PartNo'])
            if vendor_cost_cases:
                query_1 = "UPDATE product_template_listed SET vendor_cost = (CASE %s END), " %vendor_cost_cases[:-1]
                query_2 = "vendor_cost_write_date = now() at time zone 'UTC' WHERE product_tmpl_id IN %s"
                Connection.odoo_execute(query_1 +  query_2, [product_tmpl_id_list], commit=True)
            batch += 1

    # Update costs of kits whose components are in qty_res
    logging.info("Updating vendor cost of kits")
    Connection.odoo_execute("""
        UPDATE product_template_listed
        SET
            vendor_cost = RES.vendor_cost,
            vendor_cost_write_date = now() at time zone 'UTC'
        FROM (
            SELECT
                PTL.id,
                (CASE WHEN MIN(CASE WHEN PTLCOMP.vendor_cost IS NULL THEN 0 ELSE PTLCOMP.vendor_cost END) > 0 THEN SUM(PTLCOMP.vendor_cost) ELSE 0 END) as vendor_cost
            FROM product_template_listed PTL
            LEFT JOIN mrp_bom BOM ON BOM.product_tmpl_id = PTL.product_tmpl_id
            LEFT JOIN mrp_bom_line BOMLINE ON BOM.id = BOMLINE.bom_id
            LEFT JOIN product_product PP on PP.id = BOMLINE.product_id
            LEFT JOIN product_template PT on PP.product_tmpl_id = PT.id
            LEFT JOIN product_template_listed PTLCOMP ON PTLCOMP.product_tmpl_id = PT.id
            WHERE PTL.id IN (
                SELECT PTLKIT.id FROM product_template_listed PTLCOMP
                LEFT JOIN product_product PPCOMP ON PPCOMP.product_tmpl_id = PTLCOMP.product_tmpl_id
                LEFT JOIN mrp_bom_line BOMLINE ON BOMLINE.product_id = PPCOMP.id
                LEFT JOIN mrp_bom BOM ON BOM.id = BOMLINE.bom_id
                LEFT JOIN product_template_listed PTLKIT ON PTLKIT.product_tmpl_id = BOM.product_tmpl_id
                WHERE PTLKIT.product_tmpl_id IS NOT NULL AND PTLCOMP.vendor_cost_write_date >= %s
            )
            GROUP BY PTL.id
        ) as RES WHERE RES.id = product_template_listed.id
    """, [last_vendor_cost_write_date], commit = True)


def update_partially_available_kits():

    # Get partially available kits

    logging.info("Updating vendor qty and cost of partially available kits.")

    Connection.odoo_execute("""
        UPDATE product_template_listed
        SET
            vendor_qty = PARTIAL.vendor_qty,
            vendor_cost = PARTIAL.vendor_cost,
            vendor_cost_write_date = now() at time zone 'UTC',
            vendor_qty_write_date = now() at time zone 'UTC'
        FROM (
            SELECT
              PTLKIT.id,
              MIN(CASE WHEN PTLCOMP.wh_qty > 0 THEN PTLCOMP.wh_qty WHEN PTLCOMP.vendor_qty > 0 THEN PTLCOMP.vendor_qty ELSE 0 END) AS vendor_qty,
              (CASE WHEN MIN(CASE WHEN (
                    PTLCOMP.wh_shipping_cost > 0 AND PTLCOMP.wh_product_cost > 0 AND (
                      (PTLCOMP.wh_shipping_cost + PTLCOMP.wh_product_cost) < PTLCOMP.vendor_cost) OR (NOT PTLCOMP.vendor_cost > 0)
                    ) THEN (PTLCOMP.wh_shipping_cost + PTLCOMP.wh_product_cost)
                    ELSE PTLCOMP.vendor_cost END) > 0 THEN SUM(CASE WHEN (
                      PTLCOMP.wh_shipping_cost > 0 AND PTLCOMP.wh_product_cost > 0 AND
                      ((PTLCOMP.wh_shipping_cost + PTLCOMP.wh_product_cost) < PTLCOMP.vendor_cost) OR (NOT PTLCOMP.vendor_cost > 0)
                    ) THEN (PTLCOMP.wh_shipping_cost + PTLCOMP.wh_product_cost)
                    ELSE PTLCOMP.vendor_cost END) ELSE 0 END
                ) AS vendor_cost
            FROM product_template_listed PTLKIT
            LEFT JOIN product_template PTKIT ON PTKIT.id = PTLKIT.product_tmpl_id
            LEFT JOIN mrp_bom BOM ON BOM.product_tmpl_id = PTLKIT.product_tmpl_id
            LEFT JOIN mrp_bom_line BOMLINE ON BOMLINE.bom_id = BOM.id
            LEFT JOIN product_product PPCOMP ON BOMLINE.product_id = PPCOMP.id
            LEFT JOIN product_template PTCOMP ON PTCOMP.id = PPCOMP.product_tmpl_id
            LEFT JOIN product_template_listed PTLCOMP ON PTCOMP.id = PTLCOMP.product_tmpl_id
            WHERE BOM.id IS NOT NULL
            AND PTLKIT.state = 'active'
            GROUP BY PTLKIT.id HAVING (
              MIN(CASE WHEN PTLCOMP.wh_qty > 0 THEN PTLCOMP.wh_qty ELSE 0 END) = 0 AND
              MIN(CASE WHEN PTLCOMP.vendor_qty > 0 THEN PTLCOMP.vendor_qty ELSE 0 END) = 0
            )
        ) AS PARTIAL WHERE PARTIAL.id = product_template_listed.id
    """, commit=True)


def main():
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    update_vendor_qty()
    update_vendor_cost()
    update_partially_available_kits()
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

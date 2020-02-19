"""
Update Vendor cost and availability of products
"""

import argparse
import logging
import os
import sys
import slack
import simplejson
from datetime import datetime, timedelta
import opsystConnection
import getpass

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

thisdir = os.path.abspath(os.path.dirname(__file__))

fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')

LOG_FILE = '/mnt/vol/log/cron/wh_shipping_cost_%s.log' % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)

odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
stores = config.get('stores').split(',')

Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)

DT_FORMAT = '%Y-%m-%d %H:%M:%S'


def assign_average_shipping_costs_to_singles(now):

    # Singles here is possibly a kit but is stored as kit instead as components in the warehouse

    logging.info("Assigning shipping costs to WH available singles...")

    days_ago = (now - timedelta(days=60)).strftime(DT_FORMAT)

    Connection.odoo_execute("""
        UPDATE product_template_listed
        SET
            wh_prev_shipping_cost = wh_shipping_cost,
            wh_shipping_cost = FINAL.rate * 1.05, -- we decided to raise avg shipping cost cost usually it is too low
            wh_shipping_cost_write_date = now() at time zone 'UTC'
        FROM (
            SELECT PTL.id, ROUND(SHIPPING.rate::numeric, 2) as rate
            FROM product_template_listed PTL
            LEFT JOIN mrp_bom BOM ON BOM.product_tmpl_id = PTL.product_tmpl_id
            LEFT JOIN product_template PT on PT.id = PTL.product_tmpl_id
            LEFT JOIN product_product PP on PP.product_tmpl_id = PTL.product_tmpl_id
            LEFT JOIN (
                SELECT SOL.product_id, MAX(SOL.create_date) as create_date,
                (SUM(CASE WHEN SHIPPING.rate > 0 THEN SHIPPING.rate ELSE SHIPPING.computed_wh_shipping_cost END)/COUNT(DISTINCT SHIPPING.sale_id)) as rate
                FROM sale_order_line SOL
                LEFT JOIN sale_order SO ON SOL.order_id = SO.id
                LEFT JOIN (
                    SELECT PICK.sale_id, SUM(PICK.rate + PICK.rate * (CASE WHEN TLINES.count > 0 THEN TLINES.count ELSE 0 END)) as computed_wh_shipping_cost,
                    SUM(
                        (CASE WHEN RECPICK.actual_pick_shipping_price > 0 THEN RECPICK.actual_pick_shipping_price ELSE 0 END)
                        + (CASE WHEN TLINES.actual_tline_shipping_price > 0 THEN TLINES.actual_tline_shipping_price ELSE 0 END)
                    ) as rate
                    FROM stock_picking PICK
                    LEFT JOIN stock_picking_type PTYPE on PTYPE.id = PICK.picking_type_id
                    LEFT JOIN
                    (
                        SELECT PICK.id as pick_id, SUM(RECSHIP.shipping_price) as actual_pick_shipping_price
                        FROM purchase_shipping_recon_line RECSHIP
                        LEFT JOIN stock_picking PICK ON PICK.id = RECSHIP.pick_id
                        GROUP BY PICK.id
                    ) as RECPICK on PICK.id = RECPICK.pick_id
                    LEFT JOIN
                    (
                        SELECT PICK.id as pick_id, COUNT(*) as count, SUM(RECSHIPLINES.shipping_price) as actual_tline_shipping_price
                        FROM stock_picking_tracking_line TLINE
                        LEFT JOIN (
                            SELECT TLINE.id, SUM(RECSHIP.shipping_price) as shipping_price
                            FROM purchase_shipping_recon_line RECSHIP
                            LEFT JOIN stock_picking_tracking_line TLINE ON TLINE.id = RECSHIP.tracking_line_id
                            GROUP BY TLINE.id
                        ) as RECSHIPLINES on RECSHIPLINES.id = TLINE.id
                        LEFT JOIN stock_picking PICK ON PICK.id = TLINE.picking_id
                        GROUP BY PICK.id
                    ) as TLINES ON PICK.id = TLINES.pick_id
                    WHERE PTYPE.name = 'Delivery Orders' AND PICK.rate > 0 AND PICK.state = 'done'
                    GROUP BY PICK.sale_id
                ) AS SHIPPING ON SHIPPING.sale_id = SO.id
                WHERE SO.id NOT IN (
                    SELECT SO.id FROM sale_order SO
                    LEFT JOIN sale_order_line SOL on SOL.order_id = SO.id
                    GROUP BY SO.id HAVING COUNT(*) > 1
                )
                AND SOL.product_uom_qty = 1 AND SOL.create_date >= %s AND SO.state IN ('sale', 'done')
                GROUP BY SOL.product_id
            ) AS SHIPPING on SHIPPING.product_id = PP.id
            WHERE PTL.wh_qty > 0 AND SHIPPING.rate > 0 AND PP.id IN (
                SELECT QUANT.product_id FROM stock_quant QUANT
                LEFT JOIN stock_location LOC on LOC.id = QUANT.location_id
                WHERE LOC.usage = 'internal' AND LOC.name NOT IN ('Output', 'Amazon FBA') AND QUANT.qty > 0
            )
        ) AS FINAL WHERE FINAL.id = product_template_listed.id
    """, [days_ago], commit=True)



def assign_average_shipping_costs_of_kits(now):

    # Update shipping cost of kits

    logging.info("Assigning shipping costs to WH available kits...")

    days_ago = (now - timedelta(days=60)).strftime(DT_FORMAT)

    Connection.odoo_execute("""
        UPDATE product_template_listed
        SET
            wh_prev_shipping_cost = wh_shipping_cost,
            wh_shipping_cost = FINAL.rate,
            wh_shipping_cost_write_date = now() at time zone 'UTC'
        FROM (
            SELECT PTL.id, ROUND(SHIPPING.rate::numeric, 2) as rate
            FROM product_template_listed PTL
            LEFT JOIN mrp_bom BOM ON BOM.product_tmpl_id = PTL.product_tmpl_id
            LEFT JOIN product_template PT on PT.id = PTL.product_tmpl_id
            LEFT JOIN product_product PP on PP.product_tmpl_id = PTL.product_tmpl_id
            LEFT JOIN (
                SELECT SOLKIT.product_id, (SUM(CASE WHEN SHIPPING.rate > 0 THEN SHIPPING.rate ELSE SHIPPING.computed_wh_shipping_cost END)/COUNT(DISTINCT SHIPPING.sale_id)) as rate
                FROM sale_order_line_kit SOLKIT
                LEFT JOIN sale_order SO ON SOLKIT.sale_order_id = SO.id
                LEFT JOIN (
                    SELECT PICK.sale_id, SUM(PICK.rate + PICK.rate * (CASE WHEN TLINES.count > 0 THEN TLINES.count ELSE 0 END)) as computed_wh_shipping_cost,
                    SUM(
                        (CASE WHEN RECPICK.actual_pick_shipping_price > 0 THEN RECPICK.actual_pick_shipping_price ELSE 0 END)
                        + (CASE WHEN TLINES.actual_tline_shipping_price > 0 THEN TLINES.actual_tline_shipping_price ELSE 0 END)
                    ) as rate
                    FROM stock_picking PICK
                    LEFT JOIN stock_picking_type PTYPE on PTYPE.id = PICK.picking_type_id
                    LEFT JOIN
                    (
                        SELECT PICK.id as pick_id, SUM(RECSHIP.shipping_price) as actual_pick_shipping_price
                        FROM purchase_shipping_recon_line RECSHIP
                        LEFT JOIN stock_picking PICK ON PICK.id = RECSHIP.pick_id
                        GROUP BY PICK.id
                    ) as RECPICK on PICK.id = RECPICK.pick_id
                    LEFT JOIN
                    (
                        SELECT PICK.id as pick_id, COUNT(*) as count, SUM(RECSHIPLINES.shipping_price) as actual_tline_shipping_price
                        FROM stock_picking_tracking_line TLINE
                        LEFT JOIN (
                            SELECT TLINE.id, SUM(RECSHIP.shipping_price) as shipping_price
                            FROM purchase_shipping_recon_line RECSHIP
                            LEFT JOIN stock_picking_tracking_line TLINE ON TLINE.id = RECSHIP.tracking_line_id
                            GROUP BY TLINE.id
                        ) as RECSHIPLINES on RECSHIPLINES.id = TLINE.id
                        LEFT JOIN stock_picking PICK ON PICK.id = TLINE.picking_id
                        GROUP BY PICK.id
                    ) as TLINES ON PICK.id = TLINES.pick_id
                    WHERE PTYPE.name = 'Delivery Orders' AND PICK.rate > 0
                    GROUP BY PICK.sale_id
                ) AS SHIPPING ON SHIPPING.sale_id = SO.id
                WHERE SO.id NOT IN (
                    SELECT SO.id FROM sale_order SO
                    LEFT JOIN sale_order_line_kit SOLKIT on SOLKIT.sale_order_id = SO.id
                    GROUP BY SO.id HAVING COUNT(*) > 1
                )
                AND SOLKIT.product_qty = 1 AND SOLKIT.create_date >= %s AND SO.state IN ('sale', 'done')
                GROUP BY SOLKIT.product_id
            ) AS SHIPPING on SHIPPING.product_id = PP.id
            WHERE PTL.wh_qty > 0 AND BOM.id IS NOT NULL AND SHIPPING.rate > 0
        ) AS FINAL WHERE FINAL.id = product_template_listed.id
    """, [days_ago], commit=True)


def assign_pfg_shipping_costs_to_singles(now):

    logging.info("Assigning PFG shipping costs to WH available kits...")

    no_shipping_cost_ptl_rows = Connection.odoo_execute("""
        SELECT PTL.id, PT.part_number from product_template_listed PTL
        LEFT JOIN product_template PT on PTL.product_tmpl_id = PT.id
        WHERE PTL.wh_qty > 0 AND (PTL.wh_shipping_cost_write_date IS NULL OR PTL.wh_shipping_cost_write_date < %s)
        """, [now.strftime(DT_FORMAT)])

    if no_shipping_cost_ptl_rows:
        part_numbers = []
        ptl_mapping = {}
        for p in no_shipping_cost_ptl_rows:
            part_numbers.append(p['part_number'])
            ptl_mapping[p['part_number']] = p['id']

        pfg_shipping_cost_rows = Connection.autoplus_execute("""
            SELECT INV.PartNo,
            MIN(USAP.ShippingPrice + USAP.HandlingPrice) as ShippingPrice
            FROM Inventory INV
            LEFT JOIN InventoryAlt ALT on ALT.InventoryID = INV.InventoryID
            LEFT JOIN Inventory INV2 on ALT.InventoryIDAlt = INV2.InventoryID
            LEFT JOIN USAP.dbo.Warehouse USAP ON INV2.PartNo = USAP.PartNo
            WHERE INV2.MfgID IN (16,35,36,37,38,39) AND USAP.ShippingPrice > 0 AND INV.PartNo in %s
            GROUP BY INV.PartNo
        """, [part_numbers])

        # Remember that a LAD sku can possibly be mapped to multiple PFG sku...
        # ... resulting to more than one shipping rate...
        # Choose the minimal rate

        pfg_shipping_cost_mapping = {}
        for row in pfg_shipping_cost_rows:
            if row['PartNo'] in pfg_shipping_cost_mapping and row['ShippingPrice'] > 0:
                pfg_shipping_cost_mapping[row['PartNo']] = min(float(row['ShippingPrice']), pfg_shipping_cost_mapping[row['PartNo']])
            else:
                pfg_shipping_cost_mapping[row['PartNo']] = float(row['ShippingPrice'])

        pfg_shipping_cost_cases = ''
        ptl_ids = []
        for p in pfg_shipping_cost_mapping:
            pfg_shipping_cost_cases += "WHEN id = %s THEN %s " %(ptl_mapping[p], pfg_shipping_cost_mapping[p])
            ptl_ids.append(ptl_mapping[p])

        if pfg_shipping_cost_cases:
            query_1 = """
                UPDATE product_template_listed
                SET
                    wh_prev_shipping_cost = wh_shipping_cost,
                    wh_shipping_cost = (CASE %s END),
                    wh_shipping_cost_write_date = now() at time zone 'UTC'
            """ %pfg_shipping_cost_cases
            query_2 = """
                WHERE id IN %s
            """
            Connection.odoo_execute(query_1 + query_2, [ptl_ids], commit=True)


def assign_shipping_costs_to_remaining_skus(now):

    logging.info("Assigning 10.00 shipping cost to remaining SKUs...")

    Connection.odoo_execute("""
        UPDATE product_template_listed
        SET
            wh_prev_shipping_cost = wh_shipping_cost,
            wh_shipping_cost = 10,
            wh_shipping_cost_write_date = now() at time zone 'UTC'
        WHERE wh_qty > 0 AND (wh_shipping_cost_write_date IS NULL OR wh_shipping_cost_write_date < %s)
    """, [now.strftime(DT_FORMAT)], commit=True)


def oversized(now):
    # Oversized parts got fixed shipping cost
    rate = Connection.odoo_execute("""
        SELECT value FROM ir_config_parameter
        WHERE key = 'oversized.min.shipping.cost'
    """)
    logging.info("Assigning %s shipping costs to WH OVERSIZED", rate[0]['value'])
    Connection.odoo_execute("""UPDATE product_template_listed
        SET
            wh_prev_shipping_cost = wh_shipping_cost,
            wh_shipping_cost = FINAL.rate,
            wh_shipping_cost_write_date = now() at time zone 'UTC'
        FROM (
            SELECT PTL.id, %s as rate
            FROM product_template_listed PTL
            LEFT JOIN product_template PT on PT.id = PTL.product_tmpl_id
          where PT.oversized = TRUE) as FINAL WHERE FINAL.id = product_template_listed.id
    """ % rate[0]['value'], commit=True)


def main():
    now = datetime.utcnow()
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))

    oversized(now)
    assign_average_shipping_costs_to_singles(now)
    assign_average_shipping_costs_of_kits(now)

    # Not all warehouse available items will have shipping cost assigned as of this point...
    # ... since not all items have shipping history

    # Assign PFG shipping cost to singles with no shipping cost assigned yet

    assign_pfg_shipping_costs_to_singles(now)

    # Now try to assign shipping cost to kits by assigning 10.00 as shipping cost

    assign_shipping_costs_to_remaining_skus(now)
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

import pymssql
import psycopg2
import psycopg2.extras
import mysql.connector


class OpsystConnection(object):

    VENDOR_MFG_LABELS = {
        'PPR': { 'mfg_labels': "('PPR')", 'less_qty': 5 },
        'LKQ': { 'mfg_labels': "('BXLD', 'GMK')", 'less_qty': 5 },
        'PFG': { 'mfg_labels': "('BFHZ', 'REPL', 'STYL', 'NDRE', 'BOLT', 'EVFI')", 'less_qty': 5 }
    }
    SHIP_PICK_ID = 4

    def __init__(self, *args, **kwargs):
        self.odoo_db = ''
        self.odoo_db_host = ''
        self.odoo_db_user = ''
        self.odoo_db_password = ''

        for attr in kwargs.keys():
            setattr(self, attr, kwargs[attr])

    def parse_parameters_for_sql(self, params):
        sql_ready_params = []
        for param in params:
            if isinstance(param, str) or isinstance(param, unicode):
                sql_ready_params.append("'%s'" %param.replace('%', '%%'))
            elif isinstance(param, list):
                res = '(' + ', '.join("'%s'" %p.replace('%', '%%') if (isinstance(p, str) or isinstance(p, unicode)) else '%s' %p for p in param) + ')'
                sql_ready_params.append(res)
            else:
                sql_ready_params.append(param)
        return sql_ready_params

    def odoo_execute(self, query, params=[], commit=False):
        conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'"
                    %(self.odoo_db, self.odoo_db_user, self.odoo_db_host, self.odoo_db_password)
                )
        cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        sql_ready_parameters = self.parse_parameters_for_sql(params)
        final_query = query % tuple(sql_ready_parameters)
        cur.execute(final_query)
        if commit:
            conn.commit()
            return True
        else:
            results = cur.fetchall()
            return results

    def autoplus_execute(self, query, params=[]):
        conn = pymssql.connect('192.169.152.87', 'UsmanRaja', 'Nop@ss786', 'AutoPlus')
        cur = conn.cursor(as_dict=True)
        sql_ready_parameters = self.parse_parameters_for_sql(params)
        final_query = query % tuple(sql_ready_parameters)
        cur.execute(final_query)
        results = cur.fetchall()
        return results

    def ebayphantom_execute(self, query, params=[]):
        conn = mysql.connector.connect(user='usman', password='Nop@ss123',
                                host='104.238.96.196',
                                database='ebayphantom')
        cur = conn.cursor(dictionary=True)
        sql_ready_parameters = self.parse_parameters_for_sql(params)
        final_query = query % tuple(sql_ready_parameters)
        cur.execute(final_query)
        results = cur.fetchall()
        return results

    def get_store_credentials(self, stores):
        credentials = {}
        results = self.odoo_execute("SELECT * FROM sale_store WHERE code in %s", [stores])
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
            else:
                credential['max_qty'] = 80
                credential['access_id'] = res['amz_access_id']
                credential['marketplace_id'] = res['amz_marketplace_id']
                credential['seller_id'] = res['amz_seller_id']
                credential['secret_key'] = res['amz_secret_key']
            credentials[res['code']] = credential
        return credentials

    def determine_if_kit(self, part_numbers):
        """
            Returns true if a give part number is a kit
            02-1380-00034 is not a kit
            02-01380-01939 is a kit
        """
        query = """SELECT PT.part_number FROM mrp_bom BOM
            LEFT JOIN product_template PT ON PT.id = BOM.product_tmpl_id
            WHERE PT.part_number IN %s
        """
        params = [part_numbers]
        rows = self.odoo_execute(query, params)
        list_of_part_numbers = [r['part_number'] for r in rows]
        return dict((p, True if p in list_of_kits else False) for p in part_numbers)

    def get_manual_shipping_cost(self, part_numbers):
        """
            Get the manual shipping cost entered by user for each part number
        """
        query = "SELECT part_number, manual_wh_shipping_cost FROM product_template WHERE part_number IN %s"
        params = [part_numbers]
        rows =  self.odoo_execute(query, params)
        return dict((r['part_number'], float(r['manual_wh_shipping_cost']) if r['manual_wh_shipping_cost'] > 0 else 0.0) for r in rows)

    def get_non_kit_vendor_cost(self, part_numbers):
        subquery = ''
        vendor_counter = 1
        part_numbers_for_query = self.parse_parameters_for_sql([part_numbers])[0]
        for vendor in self.VENDOR_MFG_LABELS:
            subquery += """
                (SELECT INV2.PartNo as part_number, PR.Cost as cost
                FROM InventoryAlt ALT
                LEFT JOIN Inventory INV on ALT.InventoryIDAlt = INV.InventoryID
                LEFT JOIN Inventory INV2 on ALT.InventoryID = INV2.InventoryID
                LEFT JOIN InventoryMiscPrCur PR ON INV.InventoryID = PR.InventoryID
                LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
                WHERE MFG.MfgCode IN %s AND INV.QtyOnHand > %s AND INV2.PartNo IN %s)

                UNION

                (SELECT INV.PartNo as part_number, PR.Cost as cost
                FROM Inventory INV
                LEFT JOIN InventoryMiscPrCur PR ON INV.InventoryID = PR.InventoryID
                LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
                WHERE MFG.MfgCode IN %s AND INV.QtyOnHand > %s AND INV.PartNo IN %s)
            """ %(self.VENDOR_MFG_LABELS[vendor]['mfg_labels'], self.VENDOR_MFG_LABELS[vendor]['less_qty'], part_numbers_for_query,
                  self.VENDOR_MFG_LABELS[vendor]['mfg_labels'], self.VENDOR_MFG_LABELS[vendor]['less_qty'], part_numbers_for_query)
            if vendor_counter < len(self.VENDOR_MFG_LABELS):
                subquery += 'UNION'
            vendor_counter += 1

        rows = self.autoplus_execute("""
            SELECT RES.part_number, MIN(RES.cost) as cost FROM
                (
                    %s
                ) as RES GROUP BY RES.part_number
            """ %subquery
            )
        return dict((r['part_number'], float(r['cost']) if r['cost'] > 0 else 0.0) for r in rows)

    def get_non_kit_wh_shipping_cost(self, part_numbers):
        days_30_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
        wh_shipping_cost_rows = self.odoo_execute("""
            SELECT TEMPLATE.part_number,
            (CASE WHEN SHIPPING.rate > 0 THEN SHIPPING.rate ELSE 0 END) as wh_shipping_cost
            FROM (
                SELECT MOVE.product_id, (CASE WHEN SUM(MOVE.product_uom_qty) > 0 THEN SUM(PICKING.rate)/SUM(MOVE.product_uom_qty) ELSE 0 END) as rate
                FROM stock_move MOVE
                LEFT JOIN stock_picking PICKING ON PICKING.id = MOVE.picking_id
                WHERE MOVE.create_date >= %s AND PICKING.picking_type_id = %s AND PICKING.state = 'done' AND PICKING.id NOT IN (
                        SELECT PICKING2.id FROM stock_picking PICKING2
                        LEFT JOIN stock_move MOVE2 on MOVE2.picking_id = PICKING2.id
                        GROUP BY PICKING2.id HAVING COUNT(*) > 1
                    )
                GROUP BY MOVE.product_id
            ) as SHIPPING
            LEFT JOIN product_product PRODUCT on PRODUCT.id = SHIPPING.product_id
            LEFT JOIN product_template TEMPLATE on TEMPLATE.id = PRODUCT.product_tmpl_id
            WHERE TEMPLATE.part_number IN %s
        """, [days_30_ago, self.SHIP_PICK_ID, part_numbers])

        part_numbers_wh_shipping_cost = dict((r['part_number'], float(r['wh_shipping_cost']) if r['wh_shipping_cost'] > 0 else 0.0) for r in wh_shipping_cost_rows)

        part_numbers_no_shipping_cost = [p for p in part_numbers if (p not in part_numbers_wh_shipping_cost or not part_numbers_wh_shipping_cost[p]) ]

        if part_numbers_no_shipping_cost:
            pfg_shipping_cost_rows = self.autoplus_execute("""
                SELECT INV.PartNo,
                (USAP.ShippingPrice + USAP.HandlingPrice) as ShippingPrice
                FROM Inventory INV
                LEFT JOIN InventoryAlt ALT on ALT.InventoryID = INV.InventoryID
                LEFT JOIN Inventory INV2 on ALT.InventoryIDAlt = INV2.InventoryID
                LEFT JOIN USAP.dbo.Warehouse USAP ON INV2.PartNo = USAP.PartNo
                WHERE INV2.MfgID IN (16,35,36,37,38,39) AND USAP.ShippingPrice > 0 AND INV.PartNo IN %s
            """, [part_numbers_no_shipping_cost])
            part_numbers_pfg_shipping_cost = dict((r['PartNo'], float(r['ShippingPrice']) if r['ShippingPrice'] > 0 else 0.0) for r in pfg_shipping_cost_rows)
            part_numbers_wh_shipping_cost.update(part_numbers_pfg_shipping_cost)
        return part_numbers_wh_shipping_cost

    def get_non_kit_wh_base_cost(self, part_numbers):
        rows = self.odoo_execute("""
            SELECT TEMPLATE.part_number, (CASE WHEN BASE_COST.cost > 0 THEN BASE_COST.cost ELSE 0 END) as wh_base_cost,
            (CASE WHEN BASE_COST.landed_cost > 0 THEN 1 ELSE 0 END) as wh_has_landed_cost
            FROM (
                SELECT QUANT.product_id, SUM(QUANT.qty) as qty, SUM(QUANT.qty * QUANT.cost) / SUM(QUANT.qty) as cost,
                MIN(CASE WHEN (QUANT.landed_cost IS NULL OR QUANT.landed_cost = 0) AND QUANT.qty > 1 THEN 0
                    WHEN QUANT.landed_cost > 0 THEN QUANT.landed_cost
                ELSE 1 END) as landed_cost
                FROM stock_quant QUANT
                LEFT JOIN stock_location LOC on QUANT.location_id = LOC.id
                WHERE LOC.usage = 'internal' AND QUANT.cost > 0 AND QUANT.qty > 0 AND QUANT.reservation_id IS NULL
                GROUP BY QUANT.product_id
            ) as BASE_COST
            LEFT JOIN product_product PRODUCT on PRODUCT.id = BASE_COST.product_id
            LEFT JOIN product_template TEMPLATE on TEMPLATE.id = PRODUCT.product_tmpl_id
            WHERE TEMPLATE.part_number IN %s
        """, [part_numbers])
        return dict((r['part_number'], float(r['wh_base_cost']) if r['wh_has_landed_cost'] else 1.1*float(r['wh_base_cost'])) for r in rows)

    def get_non_kit_wh_cost(self, part_numbers):
        wh_base_cost_res = self.get_non_kit_wh_base_cost(part_numbers)
        wh_shipping_cost_res = self.get_manual_shipping_cost(part_numbers)
        part_numbers_with_manual_wh_shipping_cost = [p for p in wh_shipping_cost_res]
        part_numbers_with_no_manual_wh_shipping_cost = [p for p in part_numbers if p not in part_numbers_with_manual_wh_shipping_cost]
        if part_numbers_with_no_manual_wh_shipping_cost:
            wh_auto_shipping_cost_res = self.get_non_kit_wh_shipping_cost(part_numbers_with_no_wh_shipping_cost)
            wh_shipping_cost_res.update(wh_auto_shipping_cost_res)
        return dict((p, (wh_base_cost_res[p] + wh_shipping_cost_res[p]) if p in wh_shipping_cost_res and wh_shipping_cost_res[p] > 0 else 1.1 * wh_base_cost_res[p]) for p in wh_base_cost_res)

    def get_non_kit_min_cost(self, part_numbers):
        wh_cost_res = self.get_non_kit_wh_cost(part_numbers)
        vendor_cost_res = self.get_non_kit_vendor_cost(part_numbers)
        res = {}
        for p in part_numbers:
            wh_cost = wh_cost_res[p] if p in wh_cost_res else 0
            vendor_cost = vendor_cost_res[p] if p in vendor_cost_res else 0
            if wh_cost > 0 or vendor_cost > 0:
                res[p] =  min(cost for cost in [wh_cost, vendor_cost] if cost > 0)
            else:
                res[p] = 0
        return res

    def ebay_get_new_price(self, min_cost, competitor_price, repricer_type, repricer_percent, repricer_amount):
        min_cost_with_ebay_fee = min_cost / 0.88
        new_price = competitor_price - repricer_amount
        if repricer_type == 'percent':
            new_price = competitor_price * (100 - repricer_percent)/ 100
        else:
            new_price = competitor_price - repricer_amount
        return max(new_price, min_cost_with_ebay_fee)

    def get_kit_wh_shipping_cost(self, part_number):
        """
        TODO!!
        Sample kits
        TJ Fenders - 02-14104-00024
        """
        days_30_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
        wh_shipping_cost_res = self.odoo_execute("""
            SELECT (CASE WHEN SHIPPING.rate > 0 THEN SHIPPING.rate ELSE 0 END) as wh_shipping_cost
            FROM (
                SELECT MOVE.sale_orderproduct_id, (CASE WHEN SUM(MOVE.product_uom_qty) > 0 THEN SUM(PICKING.rate)/SUM(MOVE.product_uom_qty) ELSE 0 END) as rate
                FROM stock_move MOVE
                LEFT JOIN stock_picking PICKING ON PICKING.id = MOVE.picking_id
                WHERE MOVE.create_date >= '%s' AND PICKING.picking_type_id = %s AND PICKING.state = 'done' AND PICKING.id NOT IN (
                        SELECT PICKING2.id FROM stock_picking PICKING2
                    LEFT JOIN stock_move MOVE2 on MOVE2.picking_id = PICKING2.id
                    LEFT JOIN procurement_order PROC on PROC.id = MOVE2.procurement_id
                    LEFT JOIN sale_order_line
                    .procurement_id.sale_line_id.kit_line_id.product_id.product_tmpl_id
                    GROUP BY PICKING2.id HAVING COUNT(*) > 1
                    )
                GROUP BY MOVE.product_id
            ) as SHIPPING
            LEFT JOIN product_product PRODUCT on PRODUCT.id = SHIPPING.product_id
            LEFT JOIN product_template TEMPLATE on TEMPLATE.id = PRODUCT.product_tmpl_id
            WHERE TEMPLATE.part_number = '%s'
        """ %(days_30_ago, self.SHIP_PICK_ID, part_number))

        return shipping_cost

        query = """
            SELECT FROM sale_order_line_kit LKIT LEFT JOIN sale_order SO ON LKIT.sale_order_id = SO.id
            LEFT JOIN product_product PP on LKIT.product_id = PP.id
            LEFT1
        """

# -*- coding: utf-8 -*-

"""
Update WH cost and availability of products
"""

import argparse
import logging
import os
import sys
import simplejson
from datetime import datetime
import opsystConnection
import slack

logging.captureWarnings(True)
logging.getLogger('requests').setLevel(logging.ERROR)
this_module_name = os.path.basename(sys.modules['__main__'].__file__)
parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-test-only', action="store", dest="test_only")
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

LOG_FILE = os.path.join('/mnt/vol/log/cron/wh_stocks_%s.log') % datetime.today().strftime('%Y_%m')
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
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_cron_info_channel_id = config.get('slack_cron_info_channel_id')
stores = config.get('stores').split(',')
Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)


def main():
    logging.info('Updating WH stocks and prices...')
    slack.notify_slack(this_module_name, "Started cron: %s" % this_module_name)
    Connection.odoo_execute("""
        UPDATE product_template_listed
        SET
            wh_prev_qty = wh_qty,
            wh_qty = (CASE WHEN FINAL.qty >= 0 THEN FINAL.qty ELSE 0 END),
            wh_prev_product_cost = wh_product_cost,
        	wh_product_cost = (CASE WHEN FINAL.cost >= 0 THEN FINAL.cost ELSE 0 END),
        	wh_qty_write_date = (CASE WHEN wh_qty <> (CASE WHEN FINAL.qty >= 0 THEN FINAL.qty ELSE 0 END) THEN now() at time zone 'UTC' ELSE wh_qty_write_date END),
        	wh_product_cost_write_date = (CASE WHEN wh_product_cost <> (CASE WHEN FINAL.cost >= 0 THEN FINAL.cost ELSE 0 END) THEN now() at time zone 'UTC' ELSE wh_product_cost_write_date END)
        FROM (
        	SELECT
        		PTL.id,
        		PT.id as product_tmpl_id,
        		PT.name,
        		(CASE
        			WHEN SQ.qty > 0  THEN SQ.qty
        			WHEN KQ.qty > 0 THEN KQ.qty
        			ELSE 0
        		END) as qty,
        		ROUND((CASE
        	        WHEN SQ.cost > 0 THEN SQ.cost
        	        WHEN KQ.cost > 0 THEN KQ.cost
        	        ELSE 0
        	    END)::numeric, 2) as cost
        	FROM product_template_listed PTL
        	LEFT JOIN product_template PT on PTL.product_tmpl_id = PT.id
        	LEFT JOIN product_product PP on PP.product_tmpl_id = PT.id
        	LEFT JOIN (
        		SELECT DISTINCT product_tmpl_id FROM mrp_bom
        	) as BOM on BOM.product_tmpl_id = PT.id
        	LEFT JOIN (
        		SELECT QUANT.product_id, SUM(QUANT.qty) as qty,
        		SUM(QUANT.qty * (CASE WHEN QUANT.landed_cost > 0 THEN QUANT.cost ELSE 1.1 * QUANT.cost END)) / SUM(QUANT.qty) as cost
        		FROM stock_quant QUANT
        		LEFT JOIN stock_location LOC on QUANT.location_id = LOC.id
        		LEFT JOIN product_product PRODUCT on PRODUCT.id = QUANT.product_id
        		LEFT JOIN product_template TEMPLATE on TEMPLATE.id = PRODUCT.product_tmpl_id
        		WHERE LOC.usage = 'internal' AND LOC.name NOT IN ('Output', 'Amazon FBA') AND QUANT.cost > 0 AND QUANT.qty > 0 
        		-- AND (TEMPLATE.oversized IS NULL or TEMPLATE.oversized = False) 
        		AND QUANT.reservation_id IS NULL
        		GROUP BY QUANT.product_id
        	) as SQ ON SQ.product_id = PP.id
        	LEFT JOIN (
        		SELECT PT.id, MIN(CASE WHEN RES.qty IS NULL THEN 0 ELSE RES.qty END) as qty,
        	    (CASE WHEN MIN(CASE WHEN RES.cost IS NULL THEN 0 ELSE RES.cost END) > 0 THEN SUM(RES.cost) ELSE 0 END) as cost
        	    FROM mrp_bom_line BOMLINE
        	    LEFT JOIN mrp_bom BOM on BOMLINE.bom_id = BOM.id
        	    LEFT JOIN product_template PT on PT.id = BOM.product_tmpl_id
        	    LEFT JOIN (
        	    	SELECT QUANT.product_id, SUM(QUANT.qty) as qty,
        	        SUM(QUANT.qty * (CASE WHEN QUANT.landed_cost > 0 THEN QUANT.cost ELSE 1.1 * QUANT.cost END)) / SUM(QUANT.qty) as cost
        	        FROM stock_quant QUANT
        	        LEFT JOIN product_product PRODUCT on PRODUCT.id = QUANT.product_id
        	        LEFT JOIN stock_location LOC on QUANT.location_id = LOC.id
        	        LEFT JOIN product_template TEMPLATE on TEMPLATE.id = PRODUCT.product_tmpl_id
        	        WHERE LOC.usage = 'internal' AND LOC.name NOT IN ('Output', 'Amazon FBA') AND QUANT.qty > 0
        	       	-- AND (TEMPLATE.oversized IS NULL or TEMPLATE.oversized = False) 
        	       	AND QUANT.reservation_id IS NULL
        	        GROUP BY QUANT.product_id
        	    ) as RES ON RES.product_id = BOMLINE.product_id
        	    GROUP BY PT.id
        	) as KQ on KQ.id = PT.id
        ) as FINAL WHERE FINAL.id = product_template_listed.id
    """, commit=True)
    logging.info('Finished updating WH stocks and prices...')
    slack.notify_slack(this_module_name, "Ended cron: %s" % this_module_name)


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

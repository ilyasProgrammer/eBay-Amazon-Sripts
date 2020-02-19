"""
Update box cost for products in product template listed
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

thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')

LOG_FILE = '/mnt/vol/log/cron/wh_box_cost_%s.log' % datetime.today().strftime('%Y_%m')
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


def main():
    logging.info("Updating average box cost ...")
    slack.notify_slack(this_module_name, "Started cron: %s" % this_module_name)
    q = """ UPDATE product_template_listed ptl
            SET wh_box_cost = m.box_cost
            FROM (
              SELECT
                spo_pt.id AS product_tmpl_id,
                avg(boxline.quantity * PRICELIST.price) AS box_cost
              FROM stock_picking PICK
                LEFT JOIN stock_picking_packaging_line BOXLINE ON BOXLINE.picking_id = PICK.id
                LEFT JOIN product_product PP ON BOXLINE.packaging_product_id = PP.id
                LEFT JOIN product_template PT ON PT.id = PP.product_tmpl_id
                LEFT JOIN product_supplierinfo PRICELIST ON PRICELIST.product_tmpl_id = PT.id
                LEFT JOIN sale_order so ON so.id = PICK.sale_id
                LEFT JOIN stock_pack_operation spo ON spo.picking_id = pick.id
                LEFT JOIN product_product spo_pp ON spo.product_id = spo_pp.id
                LEFT JOIN product_template spo_pt ON spo_pp.product_tmpl_id = spo_pt.id
              WHERE SO.state IN ('sale', 'done')
                    AND BOXLINE.picking_id IS NOT NULL
                    AND PICK.picking_type_id IN (4, 11)
                    AND PICK.state = 'done'
                    AND PICK.create_date > '2018-01-01'  -- just cut old
                    AND PICK.id IN (SELECT sspo.id -- consider only single line SOs
                                    FROM (SELECT
                                            p.id,
                                            sum(1) qty
                                          FROM stock_pack_operation s
                                            LEFT JOIN stock_picking p ON s.picking_id = p.id
                                          WHERE p.state = 'done' AND p.create_date > '2018-01-01' -- to increase select speed
                                          GROUP BY p.id) sspo
                                    WHERE sspo.qty = 1)
              GROUP BY spo_pt.id
            ) AS m
            WHERE m.product_tmpl_id = ptl.product_tmpl_id"""
    Connection.odoo_execute(q, commit=True)
    logging.info("Done updating average box cost.")
    slack.notify_slack(this_module_name, "End cron: %s" % this_module_name)


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

# -*- coding: utf-8 -*-

"""
Update total min cost of listed products
"""

import argparse
import logging
import os
import simplejson
from datetime import datetime
import opsystConnection
import getpass
import sys
import slack

logging.captureWarnings(True)
logging.getLogger('requests').setLevel(logging.ERROR)
current_user = getpass.getuser()
this_module_name = os.path.basename(sys.modules['__main__'].__file__)
parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-test-only', action="store", dest="test_only")
args = vars(parser.parse_args())
test_only = bool(args['test_only'])
if args['test_only'] in ('0', 'False'):
    test_only = False

thisdir = os.path.abspath(os.path.dirname( __file__ ))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')

LOG_FILE = '/mnt/vol/log/cron/total_min_cost_%s.log' % datetime.today().strftime('%Y_%m')
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


def main():
    logging.info('Updating total min cost.')
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    Connection.odoo_execute("""
        UPDATE product_template_listed
        SET
            prev_total_min_cost = total_min_cost,
            total_min_cost = TOTAL.new_total_min_cost * 1.01,
            total_min_cost_write_date = now() at time zone 'UTC'
        FROM (
            SELECT
                id,
                (CASE 
                    WHEN (wh_shipping_cost > 0 AND wh_product_cost > 0 AND vendor_cost > 0 AND ((wh_shipping_cost + wh_product_cost) < vendor_cost))
                    THEN (wh_shipping_cost + wh_product_cost + wh_box_cost)
                    WHEN (vendor_cost = 0 OR vendor_cost IS NULL AND wh_shipping_cost > 0 AND wh_product_cost >0)
                    THEN  (wh_shipping_cost + wh_product_cost + wh_box_cost)
                    WHEN vendor_cost > 0 THEN vendor_cost
                    ELSE 0 END) as new_total_min_cost
            FROM product_template_listed
        ) AS TOTAL WHERE TOTAL.id = product_template_listed.id
        AND (TOTAL.new_total_min_cost <> total_min_cost
        OR total_min_cost IS NULL)
        AND state = 'active'
    """, commit=True)
    logging.info('Updated total min cost.')
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

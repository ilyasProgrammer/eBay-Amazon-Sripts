# -*- coding: utf-8 -*-

"""
Pulling TaxJar data
"""

import logging
import os
import sys
import simplejson
from datetime import datetime
import opsystConnection
import slack
import taxjar
import urllib2
import json

this_module_name = os.path.basename(sys.modules['__main__'].__file__)
thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')

LOG_FILE = os.path.join('/mnt/vol/log/cron/taxjar%s.log') % datetime.today().strftime('%Y_%m')
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
client = taxjar.Client(api_key='bc3de68be648163ba3ea37f709cb2148')


def main():
    slack.notify_slack(this_module_name, "Started cron: %s" % this_module_name)
    orders = client.list_orders({
        'from_transaction_date': '2018/01/01',
        'to_transaction_date': '2019/05/01'
    })
    order = client.show_order('#3FH81303V1239415F')
    headers = {
        # 'Content-type': 'text/xml',
        # 'Accept': 'text/plain',
        'Authorization': 'Bearer bc3de68be648163ba3ea37f709cb2148',
    }
    url = 'https://api.taxjar.com/v2/transactions/orders'
    data = {
        'from_transaction_date': '2019/04/28',
        'to_transaction_date': '2019/04/30'
    }

    r = urllib2.Request(url, 'from_transaction_date="2019/04/28" to_transaction_date="2019/04/30"', headers=headers)
    f = urllib2.urlopen(r, timeout=30)
    pass
#
# curl -G https://api.taxjar.com/v2/transactions/orders \
#   -H "Authorization: Bearer bc3de68be648163ba3ea37f709cb2148" \
#   -d from_transaction_date="2019/04/01" \
#   -d to_transaction_date="2019/04/30"

if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

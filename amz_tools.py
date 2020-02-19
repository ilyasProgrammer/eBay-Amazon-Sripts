# -*- coding: utf-8 -*-

"""Update Amazon titles"""

import argparse
import csv
import logging
import pprint
import os
import simplejson
import time
import uuid
import pickle
from datetime import datetime
import opsystConnection
import amzConnection
import getpass
import sys
import socket

current_host = socket.gethostname()
current_user = getpass.getuser()
current_path = os.path.dirname(sys.argv[0])
thisdir = os.path.abspath(os.path.dirname(__file__))
this_module_name = os.path.basename(sys.modules['__main__'].__file__)

if current_host == 'pc':
    fp = open(os.path.join(thisdir, 'test.json'), mode='r')
else:
    fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)
logs_path = config.get('logs_path')

odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials(['sinister'])


def check_feed_status():
    store = 'sinister'
    amz_request = amzConnection.AmzConnection(
        access_id=credentials[store]['access_id'],
        marketplace_id=credentials[store]['marketplace_id'],
        seller_id=credentials[store]['seller_id'],
        secret_key=credentials[store]['secret_key']
    )
    feed_id = '451063017758'
    params = {'Action': 'GetFeedSubmissionResult', 'FeedSubmissionId': feed_id}
    response = amz_request.process_amz_request('POST', '/Feeds/2009-01-01', params)
    print pprint.pformat(response)


if __name__ == "__main__":
    check_feed_status()


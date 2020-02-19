# -*- coding: utf-8 -*-

"""
Update WH cost and availability of products
"""

import argparse
import json
import logging
import os
import simplejson
import time
from datetime import datetime
import opsystConnection
import amzConnection
from slackclient import SlackClient
import getpass
import sys
import slack

current_user = getpass.getuser()
this_module_name = os.path.basename(sys.modules['__main__'].__file__)
parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-offset', action="store", dest="offset")
parser.add_argument('-test-only', action="store", dest="test_only")
args = vars(parser.parse_args())
test_only = bool(args['test_only'])
if args['test_only'] in ('0', 'False'):
    test_only = False
offset = int(args['offset']) or 0

thisdir = os.path.abspath(os.path.dirname( __file__ ))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
slack_bot_token = config.get('slack_bot_token')
slack_mapm_channel_id = config.get('slack_mapm_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')
stores = ['sinister']


Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials(stores)

LOG_FILE = '/mnt/vol/log/cron/amz_check_mapm_listings_%s.log' % datetime.today().strftime('%Y_%m')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', filename=LOG_FILE, datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger().addHandler(slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id))


def get_mapm_with_competition(store):
    asins_with_comp = []
    asin_rows = Connection.odoo_execute("""
        SELECT PL.asin FROM product_listing PL
        WHERE PL.state = 'active' AND PL.name LIKE 'MAPM%%' AND PL.store_id = %s AND PL.asin IS NOT NULL
        ORDER BY PL.id ASC
        LIMIT 50000 OFFSET %s
    """, [credentials[store]['id'], offset])

    asins = [r['asin'] for r in asin_rows]

    amz_request = amzConnection.AmzConnection(
        access_id=credentials[store]['access_id'],
        marketplace_id=credentials[store]['marketplace_id'],
        seller_id=credentials[store]['seller_id'],
        secret_key=credentials[store]['secret_key']
    )

    asins_len = len(asins)

    batches = asins_len / 10
    if asins_len % 10 == 0:
        batches -= 1

    batch = 0
    while batch <= batches:
        if batch % 100 == 0:
            logging.info('Checking ASINs of batch %s of %s...' % (batch, batches))
        batch_asins = asins[batch * 10: min(asins_len, (batch + 1) * 10)]
        params = {
            'Action': 'GetLowestOfferListingsForASIN',
            'MarketplaceId': credentials[store]['marketplace_id'],
            'ExcludeMe': 'true'
        }
        counter = 1
        for asin in batch_asins:
            params['ASINList.ASIN.' + str(counter)] = asin
            counter += 1
        while True:
            try:
                response = amz_request.process_amz_request('POST', '/Products/2011-10-01', params)
                comps = response['GetLowestOfferListingsForASINResponse']['GetLowestOfferListingsForASINResult']
                break
            except Exception as e:
                logging.error('Error: %s', e)
                time.sleep(220)
        if not isinstance(comps, list):
            comps = [comps]
        for comp in comps:
            if comp['Product']['LowestOfferListings']:
                asins_with_comp.append(comp['ASIN']['value'])
        batch += 1
        time.sleep(5)

    return asins_with_comp


def get_attachment(asins):
    now = datetime.now()
    return {
        'callback_id': 'mamp-with-competition-%s' % now.strftime('%Y%m%d%H%M%S'),
        'color': '#DC3545',
        'fallback': 'MAPM listings with competition',
        'title': 'MAPM listings with competition',
        'text': '\n'.join(asins)
    }


def notify_slack(attachment):
    sc = SlackClient(slack_bot_token)
    sc.api_call(
        "chat.postMessage",
        channel=slack_mapm_channel_id,
        as_user=False,
        username='Bender',
        text='MAPM Listings with Competition',
        attachments=[attachment]
    )
    logging.info('Notified slack: %s' % json.dumps(attachment))


def main():
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    for store in stores:
        asins = get_mapm_with_competition(store)
        if asins:
            logging.info('We have MAPM listings with competition using %s: %s' % (offset, asins))
            attachment = get_attachment(asins)
            notify_slack(attachment)
        else:
            logging.info('No MAPM listings with competition using offset %s' % offset)
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    main()


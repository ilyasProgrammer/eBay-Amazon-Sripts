"""
Check and notify through Slack if codes are properly running
"""

import argparse
import json
import logging
import os
import simplejson
from datetime import datetime
from slackclient import SlackClient
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
stores = config.get('stores').split(',')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')

LOG_FILE = '/mnt/vol/log/cron/confirm_stock_check_run_%s.log' % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)


def notify_slack(text, attachment):
    sc = SlackClient(slack_bot_token)
    sc.api_call(
        "chat.postMessage",
        channel=slack_channel_id,
        as_user=False,
        username='Bender',
        text=text,
        attachments=[attachment]
    )
    logging.info('Notified slack: %s' %json.dumps(attachment))


def get_attachment(now):
    attachment = {
        'callback_id': 'stock-check-confirmation-%s' % now.strftime('%Y%m%d%H%M%S'),
        'color': '#7CD197',
        'fallback': 'Stock check successfully ran today',
        'title': 'Stock check successfully ran today',
    }
    dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' %(now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        message = 'Stock check is yet to run today'
        attachment['color'] = '#DC3545'
        attachment['title'] = message
        attachment['fallback'] = message
        attachment['text'] = 'Something might have gone wrong. Check urgently!'
    else:
        text = ''
        files = os.listdir(dir_location)
        for store in stores:
            inv_upload_files = [f for f in files if f.startswith('%s_upload' %store)]
            text += '%s: %s inventory uploads' %(store.title(), len(inv_upload_files))
            if store == 'sinister':
                repricer_files = [f for f in files if f.startswith('%s_repricer_upload' %store)]
                shipping_files = [f for f in files if f.startswith('%s_shipping_upload' %store)]
                fbm_fba_switch_files = [f for f in files if f.startswith('%s_fbm_fba_switch_upload' %store)]
                text += ', %s repricer uploads, %s shipping template update uploads, %s FBM-FBA stock switch uploads' % (len(repricer_files), len(shipping_files), len(fbm_fba_switch_files))
            text += '\n'
        attachment['text'] = text[:-1]
    return attachment


def main():
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    now = datetime.now()
    attachment = get_attachment(now)
    notify_slack('Stock Check Confirmation', attachment)
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

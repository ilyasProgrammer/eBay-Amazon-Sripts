# -*- coding: utf-8 -*-

"""
    Script to make Digital Ocean snapshots.
    Supposed to be run under cron. Using Wiredcraft/dopy lib from github.
    Make sure all settings is valid.

    To run, install dopy: pip install dopy
"""

import argparse
import logging
import os
import simplejson
import time
from datetime import datetime
from dopy.manager import DoManager
import getpass
import sys
import slack

logging.captureWarnings(True)
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
NAME = 'opsyst_prod'  # droplet name
ID = config.get('do_droplet_id')  # droplet ID
TOKEN = config.get('do_token') # digital ocean token
DAYS_TO_KEEP = 2  # how many days keep backups. older will be deleted.
POSTFIX = '_cron_backup'

TIME_FORMAT = "%Y-%m-%d--%H-%M-%S"
NOW_STR = time.strftime(TIME_FORMAT)
LOG_FILE = '/mnt/vol/log/cron/droplet_snapshot_%s.log' % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)


def backup():
    do = DoManager(None, TOKEN, api_version=2)
    snapshot_name = NOW_STR + '_' + NAME + POSTFIX
    logging.info('Started snapshot: %s' % snapshot_name)
    res = do.snapshot_droplet(droplet_id=ID, name=snapshot_name)
    logging.info('Response: %s' % res)


def delete_old():
    now = datetime.now().date()
    do = DoManager(None, TOKEN, api_version=2)
    logging.info('Getting all images ...')
    images = do.all_images()
    for image in images:
        if POSTFIX in image['name']:
            logging.info('Found image: %s' % image['name'])
            image_date_time = image['name'].split('_', 1)[0]
            image_date = datetime.strptime(image_date_time, TIME_FORMAT).date()
            if (now - image_date).days > DAYS_TO_KEEP:
                logging.info('Deleting old image: %s' % image['name'])
                res = do.destroy_image(image_id=image['id'])
                logging.info('Deleting response: %s' % res)
            else:
                logging.info('Image is not too old. Keep it.: %s' % image['name'])


if __name__ == "__main__":
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    backup()
    delete_old()
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))

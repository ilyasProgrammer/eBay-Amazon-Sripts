# -*- coding: utf-8 -*-
"""
    Script to make Digital Ocean snapshots.
    Supposed to be run under cron. Using Wiredcraft/dopy lib from github.
    Make sure all settings is valid.
"""

from dopy.manager import DoManager
import time
import logging
from datetime import datetime

NAME = 'dev'  # droplet name
ID = 36259395  # droplet ID
DAYS_TO_KEEP = 12  # how many days keep backups. older will be deleted. 12 - two week. look delete condition.
POSTFIX = '_cron_backup'
TOKEN = 'bfdc025e6e3895c00db4ba759b0e58f447be1b0d2c816646cb99412b6a3b36ac'
TIME_FORMAT = "%Y-%m-%d--%H-%M-%S"
NOW_STR = time.strftime(TIME_FORMAT)
LOG_FORMAT = '%(asctime)s.%(msecs)03d %(levelname)s - %(funcName)s: %(message)s'
LOG_FILE = '/var/tmp/snapshot_DO_dev.py__' + NOW_STR + '.log'
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format=LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S", filemode='a')


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
    # backup()
    delete_old()



# -*- coding: utf-8 -*-

"""
Update WH cost and availability of products
"""

import argparse
import csv
import json
import logging
import os
import simplejson
import time
import uuid
import zipfile
from lxml import etree
from datetime import datetime, timedelta
import lmslib
import opsystConnection
import amzConnection
from slackclient import SlackClient
import getpass
import sys
import slack

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

odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
slack_bot_token = config.get('slack_bot_token')
# slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')
slack_critical_channel_id = config.get('slack_critical_channel_id')
stores = config.get('stores').split(',')

LOG_FILE = '/mnt/vol/log/cron/check_orders_%s.log' % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)

Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials(stores)
environment = lmslib.PRODUCTION


def start_download_job(store):
    logging.info('Starting order report download job for %s...' % store)
    download_uuid = uuid.uuid4()
    download_job = lmslib.startDownloadJob(environment, credentials[store])
    download_job.buildRequest(jobType='OrderReport', uuid=download_uuid, createdFromDaysAgo=3)
    download_job.sendRequest()
    response, resp_struct = download_job.getResponse()
    if response == 'Success':
        job_id = resp_struct.get('jobId', None)
        logging.info("Started new download job for %s with job id %s..." % (store, job_id))
        return job_id
    logging.error("Failed to start new download job for %s..." % store)
    return False


def complete_job(store, job_id):
    job_status = lmslib.GetJobStatus(environment, credentials[store])
    job_status.buildRequest(job_id)
    while True:
        response = job_status.sendRequest()
        response, resp_struct = job_status.getResponse()
        if response == 'Success':
            status = resp_struct[0].get('jobStatus', None)
            if status == 'Completed':
                logging.info("JOB COMPLETED for %s! Yey!" % store)
                return resp_struct[0].get('fileReferenceId', None)
            elif status in ('Failed', 'Aborted'):
                logging.error("Job %s for %s" % (status, store))
                return False
            time.sleep(15)
        else:
            return False


def download_file(store, job_id, file_reference_id):
    logging.info("Downloading responses from %s" % store)
    dl_file = lmslib.DownloadFile(environment, credentials[store])
    dl_file.buildRequest(job_id, file_reference_id)
    response = dl_file.sendRequest()
    response, resp_struct = dl_file.getResponse()
    if response == 'Success':
        return True
    logging.info("Failed downloading file from %s." % store)
    return False


def unzip_and_parse_downloaded_file(store, job_id, now):
    dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)

    logging.info("Unzipping the downloaded file...")
    zip_ref = zipfile.ZipFile('/mnt/vol/misc/data_responses.zip', 'r')
    zip_ref.extractall(dir_location)
    zip_ref.close()

    old_file_name = dir_location +  '/%s_report.xml' %job_id
    new_file_name = dir_location +  '/%s_order_report_%s_%s_%s.xml' %(store, now.hour, now.minute, now.second)
    os.rename(old_file_name, new_file_name)

    data_file = open(new_file_name, 'r')
    tree = etree.fromstring(data_file.read())

    orders = []
    for node in tree.getchildren():
        if node.tag.endswith('OrderReport'):
            for node1 in node.getchildren():
                if node1.tag.endswith('OrderArray'):
                    for node2 in node1.getchildren():
                        if node2.tag.endswith('Order'):
                            for node3 in node2.getchildren():
                                if node3.tag.endswith('}OrderID'):
                                    order_id = node3.text
                                if node3.tag.endswith('ShippingAddress'):
                                    for node4 in node3.getchildren():
                                        if node4.tag.endswith('}Name'):
                                            name = node4.text
                            orders.append((order_id, name,))
    return orders


def ebay_get_orders(store, now):
    job_id = start_download_job(store)
    if not job_id:
        return False
    time.sleep(10)

    file_reference_id = complete_job(store, job_id)

    if not file_reference_id:
        return False

    success = download_file(store, job_id, file_reference_id)
    if not success:
        return False

    orders = unzip_and_parse_downloaded_file(store, job_id, now)
    return orders


def amz_get_orders(store, now):
    orders = []
    amz_request = amzConnection.AmzConnection(
        access_id=credentials[store]['access_id'],
        marketplace_id=credentials[store]['marketplace_id'],
        seller_id=credentials[store]['seller_id'],
        secret_key=credentials[store]['secret_key'],
        code=store
    )

    dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)

    report_request_id = amz_request.request_report('_GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_')
    generated_report_id = amz_request.get_status_of_request(report_request_id)
    res_file_name = dir_location + '/%s_order_report_%s_%s_%s.tsv' % (store, now.hour, now.minute, now.second)
    amz_request.get_report(generated_report_id, res_file_name)
    purchase_date_limit = (now - timedelta(minutes=60)).strftime('%Y-%m-%d' + 'T' + '%H:%M:%S' + '+00:00')
    counter = 1
    with open(res_file_name) as tsv:
        for line in csv.reader(tsv, dialect="excel-tab", quoting=csv.QUOTE_NONE):
            if counter == 1:
                counter += 1
                continue
            if line and line[4] <= purchase_date_limit:
                orders.append((line[0], line[16], line[7]))
            counter += 1
    return orders


def get_missing_orders(store, orders):
    missing_orders = []
    orders_len = len(orders)
    batches = orders_len / 100
    if orders_len % 100 == 0:
        batches -= 1
    batch = 0
    while batch <= batches:
        logging.info('Checking %s orders batch %s of %s...' % (store, batch, batches))
        batch_orders = orders[batch * 100: min(orders_len, (batch + 1) * 100)]
        order_ids = [order[0] for order in batch_orders]
        rows = Connection.odoo_execute("""
            SELECT web_order_id FROM sale_order WHERE web_order_id IN %s AND STATE NOT IN ('cancel')
            AND store_id = %s 
        """, [order_ids, credentials[store]['id']])
        existing_order_ids = [r['web_order_id'] for r in rows]
        batch_missing_orders = [order for order in batch_orders if order[0] not in existing_order_ids]
        missing_orders += batch_missing_orders
        batch += 1
    return missing_orders


def get_attachment(now, missing_orders):
    attachment = {
        'callback_id': 'missing-order-%s' % now.strftime('%Y%m%d%H%M%S'),
        'color': '#7CD197',
        'fallback': 'We have no unrecorded orders',
        'title': 'We have no unrecorded orders',
        'text': 'Awesome day!'
    }
    total_missing_orders = sum(len(missing_orders[store]) for store in missing_orders)
    if total_missing_orders > 0:
        message = 'We have %s unrecorded orders.' % total_missing_orders
        attachment['color'] = '#DC3545'
        attachment['title'] = message
        attachment['fallback'] = message
        text = ''
        for store in missing_orders:
            text += '%s (%s orders)\n' % (store.title(), len(missing_orders[store]))
            if credentials[store]['type'] == 'ebay':
                for order in missing_orders[store]:
                    text += '%s | %s\n' % (order[0], order[1].decode(errors='replace'))
            else:
                for order in missing_orders[store]:
                    text += '%s | %s | %s\n' % (order[0], order[1].decode(errors='replace'), order[2])
        attachment['text'] = text[:-1]
    return attachment


def notify_slack(attachment):
    sc = SlackClient(slack_bot_token)
    sc.api_call(
        "chat.postMessage",
        channel=slack_critical_channel_id,
        as_user=False,
        username='Bender',
        text='Missing Orders Notification',
        attachments=[attachment]
    )
    logging.info('Notified slack: %s' % json.dumps(attachment))


def main():
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    now = datetime.now()
    missing_orders = {}
    for store in stores:
        orders = []
        if credentials[store]['type'] == 'ebay':
            orders = ebay_get_orders(store, now)
        else:
            orders = amz_get_orders(store, now)
        missing_orders[store] = get_missing_orders(store, orders)
    attachment = get_attachment(now, missing_orders)
    notify_slack(attachment)
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

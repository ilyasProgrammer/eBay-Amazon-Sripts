"""
Upload updated stocks and ebay prices. AKA Repricer.
"""

import argparse
import json
import logging
import lmslib
import os
import simplejson
import time
import uuid
import zipfile
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
parser.add_argument('-from', action="store", dest="from")  # for compatibility with v2_update_ebay_competitor_prices.py
parser.add_argument('-to', action="store", dest="to")  # for compatibility with v2_update_ebay_competitor_prices.py
parser.add_argument('-test-only', action="store", dest="test_only")
args = vars(parser.parse_args())
test_only = bool(args['test_only'])
if args['test_only'] in ('0', 'False'):
    test_only = False

thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)
with_price_change_list = []
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')
stores = config.get('stores').split(',')
# stores = ['visionary']

LOG_FILE = '/mnt/vol/log/cron/ebay_repricer_%s.log' % datetime.today().strftime('%Y_%m')
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


def get_and_summarize_listings(store):
    rows = []
    rates = {'visionary': 1.08,
             'visionary_mg': 1.07,
             'visionary_v2f': 1.06,
             'revive': 1.09,
             'revive_apc': 1.095,
             'rhino': 1.1,
             'ride': 1.11}
    # Justin 04-01-2018: So for visionary make it 3%, 2% for ride, and 1.5% for revive
    percents = {'visionary': 3,
                'visionary_mg': 2.9,
                # 'visionary_v2f': 3,  # competes only with C2C
                'revive': 1.5,
                'revive_apc': 1.4,
                'rhino': 0.5,
                'ride': 2}
    ebay_fees = {'visionary': 0.1051, 'revive': 0.1103, 'rhino': 0.1303, 'ride': 0.0847}
    rate_diff = 1 + (rates[store] - rates['visionary'])
    paypal_fee = 0.0215

    qry = """SELECT PL.name,coalesce(PL.max_store_qty,0) as max_store_qty,PL.dumping_percent,PL.dumping_amount,PL.custom_label,
                    PL.current_price,PTL.wh_qty,PTL.vendor_qty,PTL.total_min_cost,PL.do_not_reprice, PL.keep_manual, PL.manual_price,
                    PL.do_not_restock,PL.ebay_reprice_against_competitors,PL.sell_with_loss,PL.sell_with_loss_type,
                    PL.sell_with_loss_percent,PL.sell_with_loss_amount,PTL.wh_sale_price,REP.type,REP.percent,
                    REP.amount,COMP.non_c2c_comp_price,COMP.c2c_comp_price, 
                    pb.id as pb_id, pb.multiplier, pb.do_not_reprice as pb_do_not_reprice, pb.use_supplier_price
             FROM product_template_listed PTL
             LEFT JOIN repricer_competitor RC
             ON PTL.product_tmpl_id = RC.product_tmpl_id
             LEFT JOIN product_listing PL
             ON PTL.product_tmpl_id = PL.product_tmpl_id
             LEFT JOIN product_brand pb
             ON pl.brand_id = pb.id
             LEFT JOIN repricer_scheme REP
             ON REP.id = PL.repricer_scheme_id
             LEFT JOIN ( SELECT COMP.product_tmpl_id,
                                MIN(CASE WHEN COMP.seller <> 'classic2currentfabrication' THEN COMP.price ELSE NULL END) AS non_c2c_comp_price,
                                MIN(CASE WHEN COMP.seller = 'classic2currentfabrication' THEN COMP.price ELSE NULL END) AS c2c_comp_price
                         FROM repricer_competitor COMP
                         WHERE COMP.state = 'active' AND COMP.price > 0
                         GROUP BY COMP.product_tmpl_id
                        ) AS COMP
             ON COMP.product_tmpl_id = PTL.product_tmpl_id"""
    if with_price_change_list:
        qry += """ WHERE PL.store_id = %s AND PL.state = 'active' AND RC.item_id in %s""" % (credentials[store]['id'], list_to_sql_str(with_price_change_list))
    else:
        qry += """ WHERE PL.store_id = %s AND PL.state = 'active' """ % credentials[store]['id']
    listings = Connection.odoo_execute(qry)
    for l in listings:
        qty = -1
        if not (l['do_not_restock'] and l['custom_label'] and l['custom_label'].startswith('X-')):
            qty = 0
            if not l['total_min_cost']:
                qty = 0
            elif l['wh_qty'] > 0 and l['vendor_qty'] > 0:
                qty = l['wh_qty'] + l['vendor_qty']
            elif l['wh_qty'] > 0:
                qty = l['wh_qty']
            elif l['vendor_qty'] > 0:
                qty = l['vendor_qty']
            if int(l['max_store_qty']):  # Individual listing limit has higher priority
                if qty > int(l['max_store_qty']):
                    qty = int(l['max_store_qty'])
            elif qty > credentials[store]['max_qty']:
                qty = credentials[store]['max_qty']

        price = -1
        if not l['do_not_reprice'] and qty > 0 and l['total_min_cost'] > 0:
            ebay_fee = ebay_fees[store] if store in ebay_fees else 0.11
            min_ebay_cost = (0.03 + l['total_min_cost']) / (1 - ebay_fee - paypal_fee)  # and then add 5% to the min ebay and min amazon
            if l['use_supplier_price']:
                price = l['total_min_cost']
            elif l['pb_id'] and l['pb_do_not_reprice'] and l['multiplier']:
                price = min_ebay_cost * l['multiplier']
            else:
                if l['sell_with_loss']:
                    if l['sell_with_loss_type'] == 'percent':
                        percent = l['sell_with_loss_percent'] if l['sell_with_loss_percent'] > 0 else 1
                        min_ebay_cost = (100 - percent) / 100 * min_ebay_cost
                    else:
                        amount = l['sell_with_loss_amount'] if l['sell_with_loss_amount'] > 0 else 1
                        min_ebay_cost = min_ebay_cost - amount
                price = min_ebay_cost * rates[store]
                if l['custom_label'] and l['custom_label'].startswith('X-'):
                    price = -1
                elif l['custom_label'] and l['custom_label'].startswith('MG-'):
                    if l['wh_qty'] > 0 and l['wh_sale_price'] > 0:
                        price = max((1 + (rates['visionary_mg'] - rates['visionary'])) * l['wh_sale_price'], min_ebay_cost)
                    else:
                        price = min_ebay_cost * rates['visionary_mg']
                elif l['custom_label'] and l['custom_label'].startswith('V2F'):
                    if l['wh_qty'] > 0 and l['wh_sale_price'] > 0:
                        price = max((1 + (rates['visionary_v2f'] - rates['visionary'])) * l['wh_sale_price'], min_ebay_cost)
                    else:
                        price = min_ebay_cost * rates['visionary_v2f']
                elif l['custom_label'] and l['custom_label'].startswith('APC'):
                    if l['wh_qty'] > 0 and l['wh_sale_price'] > 0:
                        price = max((1 + (rates['revive_apc'] - rates['visionary'])) * l['wh_sale_price'], min_ebay_cost)
                    else:
                        price = min_ebay_cost * rates['revive_apc']
                else:
                    # Dont pull down price of wh available listing
                    if price > 0 and l['wh_qty'] > 0:
                        if l['wh_sale_price'] > 0:
                            price = max(rate_diff * l['wh_sale_price'], min_ebay_cost)
                        elif l['current_price'] > 0:
                            price = max(l['current_price'], min_ebay_cost)

                if price > 0 and l['ebay_reprice_against_competitors'] and (l['non_c2c_comp_price'] > 0 or l['c2c_comp_price'] > 0):
                    type = l['type'] if l['type'] else 'percent'
                    comp_price = l['non_c2c_comp_price']
                    if store == 'rhino' or (l['custom_label'] and l['custom_label'].startswith('V2F')):
                        comp_price = l['c2c_comp_price']
                    if comp_price > 0:
                        if l['dumping_percent']:
                            price = ((100 - l['dumping_percent']) / 100.0) * comp_price
                        elif l['dumping_amount']:
                            price = comp_price - l['dumping_amount']
                        elif type == 'percent':
                            percent = percents[store] if percents.get(store) else 1
                            price = ((100 - percent) / 100.0) * comp_price
                        else:
                            amount = l['amount'] if l['amount'] > 0 else 1
                            price = comp_price - amount
                    price = max(price, min_ebay_cost)
                    if l['keep_manual'] and l['manual_price'] > 0 and comp_price > 0:
                        if min_ebay_cost < l['manual_price'] < comp_price:
                            price = l['manual_price']
                        else:
                            logging.warning("Listing has keep_manual=TRUE incorrect! %s", l)

        rows.append((l['name'], qty, price,  l['current_price'] or 0))  # 29-11-2018 So to 7% for all brands and listings
    return rows


def get_xml_content(store):
    rows = []
    rows = get_and_summarize_listings(store)

    if not rows:
        return False

    inner_counter = 0
    item_update = ''
    item_group_update = "<ReviseInventoryStatusRequest xmlns='urn:ebay:apis:eBLBaseComponents'><Version>941</Version>%s</ReviseInventoryStatusRequest>\n"
    xml_content_body = ''
    msg = 'Repricer updating prices:\n'
    for row in rows:
        if row[1] >= 0 or row[2] >= 0:
            item_update += "<InventoryStatus><ItemID>%s</ItemID>" % row[0]
            if row[1] >= 0:
                item_update += "<Quantity>%s</Quantity>" % row[1]
            if row[2] > 0 and row[1] > 0:
                item_update += "<StartPrice>%s</StartPrice>" % '{0:.2f}'.format(row[2])
            item_update += "</InventoryStatus>"
            inner_counter += 1
        if inner_counter % 4 == 0 and item_update:
            xml_content_body += item_group_update % item_update
            item_update = ''
        # logging.info(row)
        if float(row[3]) != float('{0:.2f}'.format(row[2])) and float('{0:.2f}'.format(row[2])) > 0:
            s = "%s -> %s  Listing: %s Qty:%s \n" % (row[3], '{0:.2f}'.format(row[2]), row[0], row[1])
            if s not in msg:
                msg += s

    if item_update:
        xml_content_body += item_group_update % item_update

    if xml_content_body:
        xml_content = "<?xml version='1.0' encoding='UTF-8'?>"
        xml_content += "<BulkDataExchangeRequests>"
        xml_content += "<Header>"
        xml_content += "<Version>941</Version>"
        xml_content += "<SiteID>100</SiteID>"
        xml_content += "</Header>\n"
        xml_content += xml_content_body
        xml_content += "</BulkDataExchangeRequests>"
    if msg != 'Repricer updating prices:\n':
        slack.notify_slack(this_module_name, msg)
    return xml_content


def build_upload_file(store, now):
    dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)
    upload_file_name = dir_location + '/%s_upload_%s_%s_%s.xml' % (store, now.hour, now.minute, now.second)
    xml_content = get_xml_content(store)
    upload_file = open(upload_file_name, 'wb')
    upload_file.write(xml_content)
    upload_file.close()
    if xml_content:
        return upload_file_name
    logging.info('No file to upload for %s' % store)
    return False


def get_created_jobs(store):
    logging.info("Getting created job for %s" % store)
    get_jobs = lmslib.GetJobs(environment, credentials[store])
    get_jobs.buildRequest('ReviseInventoryStatus', 'Created')
    response = get_jobs.sendRequest()
    response, resp_struct = get_jobs.getResponse()
    if response == 'Success' and type(resp_struct) == list and len(resp_struct) > 0:
        return resp_struct[0].get('jobId', None)
    return False


def abort_created_job(store, created_job_id):
    logging.info("Aborting job %s in %s" %(created_job_id, store))
    abort_job = lmslib.AbortJob(environment, credentials[store])
    abort_job.buildRequest(created_job_id)
    response = abort_job.sendRequest()
    response, resp_struct = abort_job.getResponse()
    if response == 'Error':
        logging.error('Aborting job failed: %s' % resp_struct.get('message', None))


def create_new_upload_job(store):
    errors = 0
    while True:
        upload_uuid = uuid.uuid4()
        create_job = lmslib.CreateUploadJob(environment, credentials[store])
        create_job.buildRequest('ReviseInventoryStatus', 'gzip', upload_uuid)
        response = create_job.sendRequest()
        response, resp_struct = create_job.getResponse()

        if response == 'Success':
            job_id = resp_struct.get('jobId', None )
            file_reference_id = resp_struct.get( 'fileReferenceId', None )
            logging.info("Created new upload job for %s with job id %s and file reference id %s..." % (store, job_id, file_reference_id))
            return job_id, file_reference_id
        else:
            logging.warning("%s %s" % (store, response))
            created_job_id = get_created_jobs(store)
            if created_job_id:
                abort_created_job(store, created_job_id)
            else:
                errors += 1
                logging.info('Errors: %s Sleep 300 ...', errors)
                time.sleep(300)
            if errors == 10:
                return False


def upload_file(store, job_id, file_reference_id, filename):
    logging.info("Uploading file to %s..." % store)
    upload_job = lmslib.UploadFile(environment, credentials[store])
    upload_job.buildRequest(job_id, file_reference_id, filename)
    response = upload_job.sendRequest()
    response, response_struct = upload_job.getResponse()

    if response == 'Success':
        return True
    logging.error("Upload file to %s failed.")
    return False


def start_job(store, job_id):
    logging.info("Starting job processing for %s." % store)
    start_job = lmslib.StartUploadJob(environment, credentials[store])
    start_job.buildRequest(job_id)
    response = start_job.sendRequest()
    response, response_dict = start_job.getResponse()

    if response == 'Success':
        return True
    logging.error("Starting job for %s failed." % store)
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
                logging.info("JOB %s COMPLETED for %s! Yey!", job_id, store)
                return resp_struct[0].get('fileReferenceId', None)
            elif status in ('Failed', 'Aborted'):
                logging.error("Job %s for %s" % (status, store))
                return False
            time.sleep(60)
        else:
            logging.warning('Not ready yet. Status: %s', resp_struct[0].get('jobStatus', None))
            return False


def download_response_file(store, now, job_id, download_file_id):
    logging.info("Downloading responses for %s with download file id %s..." % (store, download_file_id))
    download_file = lmslib.DownloadFile(environment, credentials[store])
    download_file.buildRequest(job_id, download_file_id)
    response = download_file.sendRequest()
    # logging.info("Response sendRequest: %s", response)
    path = '/mnt/vol/misc/%s_response.zip' % datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
    logging.info("Path: %s", path)
    time.sleep(30)
    response, resp_struc = download_file.getResponse(path)
    logging.info("Response getResponse: %s", response)
    logging.info("resp_struc: %s", resp_struc)
    if response == 'Success':
        try:
            logging.info("Successfully downloaded response!")
            dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' % (now.year, now.month, now.day)
            logging.info("Extracted to: %s", dir_location)
            zip_ref = zipfile.ZipFile(path, 'r')
            zip_ref.extractall(dir_location)
            zip_ref.close()
            os.remove(path)
            old_file_name = dir_location + '/%s_responses.xml' % job_id
            new_file_name = dir_location + '/%s_responses_%s_%s_%s.xml' % (store, now.hour, now.minute, now.second)
            os.rename(old_file_name, new_file_name)
        except Exception as e:
            logging.error(e)
            os.remove(path)
    else:
        logging.error("Downloading file from %s failed." % store)


def main(ids_to_update=[]):
    global with_price_change_list
    if ids_to_update:
        with_price_change_list = ids_to_update
        slack.notify_slack('Repricer', "Repricer called from competitors prices update script.")
        slack.notify_slack('Repricer', "Updating ebay items ids: %s" % with_price_change_list)
    else:
        slack.notify_slack('Repricer', "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    now = datetime.now()
    logging.info('Stores to proceed: %s', str(stores))
    for store in stores:
        if credentials[store]['type'] == 'ebay':

            upload_file_name = build_upload_file(store, now)
            if not upload_file_name:
                continue
            res = create_new_upload_job(store)
            if not res:
                logging.error('Cant create job for %s', store)
                return False
            job_id, file_reference_id = res

            time.sleep(10)
            success = upload_file(store, job_id, file_reference_id, upload_file_name)
            if not success:
                continue

            time.sleep(10)
            success = start_job(store, job_id)
            if not success:
                continue

            time.sleep(10)

            download_file_id = complete_job(store, job_id)
            if not download_file_id:
                continue

            download_response_file(store, now, job_id, download_file_id)
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


def list_to_sql_str(data_list):
    qry = ''
    for el in data_list:
        qry += "'" + el + "' ,"
    qry = '(' + qry[:-1] + ')'
    return qry


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")


# -*- coding: utf-8 -*-

"""
Download active inventory reports from Amazon and eBay.
Make sure that each listing is mapped to a LAD sku and has active status
Send an email notification if there are unmapped listings
Product listings not in the in the inventory reports should be marked as inactive/ended
Create/update listed products record. Mark listed products as active or inactive depending on whether or not a product has existing active listing

Note that there are kits whose one or more components are not necessarily listed.
In such cases, for purpose of computing costs and availability, we should ensure that there are active listed product records for the components
"""

import argparse
import csv
import lmslib
import logging
import sys
import os
import simplejson
import time
import uuid
import zipfile
from datetime import datetime
from lxml import etree
import opsystConnection
import amzConnection
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

thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)
logs_path = config.get('logs_path')

odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
stores = config.get('stores').split(',')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')

LOG_FILE = os.path.join(logs_path + 'product_listings_%s.log') % datetime.today().strftime('%Y_%m')
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
    logging.info('Starting inventory download job for %s...' %store)
    download_uuid = uuid.uuid4()
    download_job = lmslib.startDownloadJob(environment, credentials[store])
    download_job.buildRequest(jobType='ActiveInventoryReport', uuid=download_uuid)
    download_job.sendRequest()
    response, resp_struct = download_job.getResponse()
    if response == 'Success':
        job_id = resp_struct.get('jobId', None)
        logging.info("Started new download job for %s with job id %s..." %(store, job_id))
        return job_id
    logging.error("Failed to start new download job for %s..." %(store))
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
                logging.info("JOB COMPLETED for %s! Yey!" %store)
                return resp_struct[0].get('fileReferenceId', None)
            elif status in ('Failed', 'Aborted'):
                logging.error("Job %s for %s" %(status, store))
                return False
            time.sleep(15)
        else:
            return False


def download_file(store, job_id, file_reference_id):
    logging.info("Downloading responses from %s" %store)
    dl_file = lmslib.DownloadFile(environment, credentials[store])
    dl_file.buildRequest(job_id, file_reference_id)
    response = dl_file.sendRequest()
    response, resp_struct = dl_file.getResponse()
    if response == 'Success':
        return True
    logging.info("Failed downloading file from %s." %store)
    return False


def unzip_and_parse_downloaded_file(store, job_id, now):
    dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' %(now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)

    logging.info("Unzipping the downloaded file...")
    zip_ref = zipfile.ZipFile('/mnt/vol/misc/data_responses.zip', 'r')
    zip_ref.extractall(dir_location)
    zip_ref.close()

    old_file_name = dir_location +  '/%s_report.xml' %job_id
    new_file_name = dir_location +  '/%s_report_%s_%s_%s.xml' %(store, now.hour, now.minute, now.second)
    os.rename(old_file_name, new_file_name)

    data_file = open(new_file_name, 'r')
    tree = etree.fromstring( data_file.read() )

    res_file_name = dir_location + '/' + '%s_active_inventory_report_%s_%s_%s.csv' %(store, now.hour, now.minute, now.second)
    with open(res_file_name, 'wb') as res_file:
        writer = csv.writer(res_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for node in tree.getchildren():
            if node.tag.endswith('ActiveInventoryReport'):
                for child in node.getchildren():
                    if child.tag.endswith('SKUDetails'):
                        for thing in child.getchildren():
                            if thing.tag.endswith('ItemID'):
                                ItemID = thing.text
                            if thing.tag.endswith('SKU'):
                                SKU = thing.text
                            if thing.tag.endswith('Price'):
                                Price = thing.text
                            if thing.tag.endswith('Quantity'):
                                Quantity = thing.text
                        writer.writerow([ItemID, SKU, Price, Quantity])
    return res_file_name


def ebay_download_inventory_report(store, now):

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

    res_file_name = unzip_and_parse_downloaded_file(store, job_id, now)
    return res_file_name


def update_current_prices_of_listings(store, res_file_name):
    logging.info('Updating current prices of %s.' %store)
    counter = 1
    rows = []
    if credentials[store]['type'] == 'ebay':
        with open(res_file_name, 'rb') as res_file:
            reader = csv.reader(res_file, delimiter=',', quotechar='"')
            for row in reader:
                rows.append( (row[0], float(row[2]) ))
    else:
        with open(res_file_name) as tsv:
            for row in csv.reader(tsv, dialect="excel-tab"):
                if counter == 1:
                    counter += 1
                    continue
                rows.append((row[0], float(row[2])))

    rows_len = len(rows)
    batches = rows_len / 1000
    if rows_len % 1000 == 0:
        batches -= 1

    logging.info("To update current prices of %s listings to activate in %s batches" % (rows_len, batches))
    batch = 0
    while batch <= batches:
        if batch % 50 == 0 and batch > 0:
            logging.info("Finished updating batch %s of %s..." % (batch, batches))
        batch_rows = rows[batch * 1000: min(rows_len, (batch + 1) * 1000)]
        item_ids = []
        price_cases = ''
        price_cases2 = ''
        for row in batch_rows:
            item_ids.append(row[0])
            price_cases += "WHEN name='%s' THEN %s " % (row[0], row[1])
            price_cases2 += "WHEN name='%s' THEN TO_CHAR(NOW() AT TIME ZONE 'EST', 'YYYY-MM-DD HH24:MI:SS') || '  ' || ROUND(%s, 2) ||E'\n' || coalesce(price_history, '') " % (row[0], row[1])
        query_1 = """UPDATE product_listing 
                     SET current_price = (CASE %s END), price_history = (CASE %s END)
                     WHERE store_id = %s""" % (price_cases, price_cases2, credentials[store]['id'])
        query_2 = " AND name IN %s"
        if price_cases:
            qry = query_1 + query_2
            logging.info(qry)
            logging.info(item_ids)
            Connection.odoo_execute(qry, [item_ids], commit=True)
        batch += 1


def ebay_update_listings(store, res_file_name):

    opsyst_active_listings_rows = Connection.odoo_execute("""
        SELECT LISTING.name FROM product_listing LISTING
        WHERE LISTING.state = 'active' AND LISTING.store_id = %s
    """, [credentials[store]['id']])
    opsyst_active_listings_list = []
    for r in opsyst_active_listings_rows:
        opsyst_active_listings_list.append(r['name'])

    ebay_active_listings_dict = {}

    with open(res_file_name, 'rb') as res_file:
        reader = csv.reader(res_file, delimiter=',', quotechar='"')
        for row in reader:
            listing_dict = {row[0]: (row[1], row[2])}
            ebay_active_listings_dict = dict(ebay_active_listings_dict, **listing_dict)

    logging.info('Computing inactive listings.')
    inactive_listings = [item_id for item_id in opsyst_active_listings_list if item_id not in ebay_active_listings_dict]
    if inactive_listings:
        logging.info('Setting listings to inactive.')
        Connection.odoo_execute("""
            UPDATE product_listing SET state = 'ended' WHERE name IN %s
        """, [inactive_listings], commit=True)

    logging.info('Computing  listings to reactivate or create.')
    listings_to_activate = [item_id for item_id in ebay_active_listings_dict if item_id not in opsyst_active_listings_list]
    unmapped_listings = []
    if listings_to_activate:
        listings_to_activate_len = len(listings_to_activate)
        batches = listings_to_activate_len / 1000
        if listings_to_activate_len % 1000 == 0:
            batches -= 1

        logging.info("To process %s listings to activate in %s batches" %(listings_to_activate_len, batches))
        batch = 0
        while batch <= batches:
            logging.info("Processing batch %s." %(batch))
            batch_list = listings_to_activate[batch * 1000: min(listings_to_activate_len, (batch + 1) * 1000)]

            # First, reactivate product_listings that are ended

            batch_listings_to_reactivate_rows = Connection.odoo_execute("""
                SELECT name FROM product_listing WHERE name IN %s
            """, [batch_list])

            batch_listings_to_reactivate_list = []
            if batch_listings_to_reactivate_rows:
                for p in batch_listings_to_reactivate_rows:
                    batch_listings_to_reactivate_list.append(p['name'])
                Connection.odoo_execute("""
                    UPDATE product_listing SET state = 'active' WHERE name IN %s
                """, [batch_listings_to_reactivate_list], commit=True)

            batch_listings_to_create = [item_id for item_id in batch_list if item_id not in batch_listings_to_reactivate_list]

            values = ''
            for r in batch_listings_to_create:
                listing = ebay_active_listings_dict[r]
                custom_label = listing[0]
                price = float(listing[1])
                product_tmpl_id = False
                if custom_label.startswith('APC') or custom_label.startswith('MG'):
                    brand_id = 42 # APC brand_id
                    if custom_label.startswith('MG'):
                        brand_id = 40
                    autoplus_row = Connection.autoplus_execute("""SELECT INV.PartNo as LADPartNo
                        FROM Inventory INV
                        LEFT JOIN InventoryAlt ALT on ALT.InventoryIdAlt = INV.InventoryId
                        LEFT JOIN Inventory INV2 on ALT.InventoryId = INV2.InventoryId
                        WHERE INV2.PartNo = %s AND INV2.MfgID = %s and INV.MfgID = 1
                    """, [custom_label, brand_id])
                    if autoplus_row:
                        product_tmpl_row = Connection.odoo_execute("SELECT PT.id FROM product_template PT WHERE PT.part_number = %s", [autoplus_row[0]['LADPartNo']])
                        if product_tmpl_row:
                            product_tmpl_id = product_tmpl_row[0]['id']
                else:
                    lad_sku = custom_label
                    if custom_label.startswith('X') or custom_label.startswith('V2F'):
                        lad_sku = custom_label.replace('X-', '').replace('V2F-', '')
                    product_tmpl_row = Connection.odoo_execute("SELECT PT.id FROM product_template PT WHERE PT.part_number = %s", [lad_sku])
                    if product_tmpl_row:
                        product_tmpl_id = product_tmpl_row[0]['id']
                if product_tmpl_id:
                    values += "('%s', '%s', %s, %s, %s, 'active')," %(r, custom_label, product_tmpl_id, price, credentials[store]['id'])
                else:
                    unmapped_listings.append(r)
            if values:
                Connection.odoo_execute("""
                    INSERT INTO product_listing (name, custom_label, product_tmpl_id, current_price, store_id, price_history ) VALUES
                    %s
                  , TO_CHAR(NOW() AT TIME ZONE 'EST', 'YYYY-MM-DD HH24:MI:SS') || '  ' || ROUND(current_price, 2) ||E'\n' || coalesce(price_history, '')""" % values[:-1], commit=True)
            batch += 1
    return unmapped_listings


def amazon_download_inventory_report(store, now):

    amz_request = amzConnection.AmzConnection(
        access_id=credentials[store]['access_id'],
        marketplace_id=credentials[store]['marketplace_id'],
        seller_id=credentials[store]['seller_id'],
        secret_key=credentials[store]['secret_key']
    )

    def request_report():
        requestreport_params = {}
        requestreport_params['Action'] = 'RequestReport'
        requestreport_params['ReportType'] = '_GET_FLAT_FILE_OPEN_LISTINGS_DATA_'
        response = amz_request.process_amz_request('GET', '/Reports/2009-01-01', requestreport_params)
        ReportRequestId = response['RequestReportResponse']['RequestReportResult']['ReportRequestInfo']['ReportRequestId']['value']
        logging.info('Requested report id %s for %s' %(ReportRequestId, store))
        return ReportRequestId

    def get_status_of_request(ReportRequestId):
        reportrequest_list_params = {}
        reportrequest_list_params['Action'] = 'GetReportRequestList'
        reportrequest_list_params['ReportRequestIdList.Id.1'] = ReportRequestId
        while True:
            response = amz_request.process_amz_request('GET', '/Reports/2009-01-01', reportrequest_list_params)
            status = response['GetReportRequestListResponse']['GetReportRequestListResult']['ReportRequestInfo']['ReportProcessingStatus']['value']
            if status == '_DONE_':
                GeneratedReportId = response['GetReportRequestListResponse']['GetReportRequestListResult']['ReportRequestInfo']['GeneratedReportId']['value']
                logging.info('Generated report id %s for %s' %(GeneratedReportId, store))
                return GeneratedReportId
            time.sleep(60)

    def get_report(ReportRequestId, filepath):
        getreport_params = {}
        getreport_params['Action'] = 'GetReport'
        getreport_params['ReportId'] = ReportRequestId
        response = amz_request.process_amz_request('POST', '/Reports/2009-01-01', getreport_params)
        inv_report_file = open(filepath, 'wb')
        inv_report_file.write(response)
        inv_report_file.close()

    ReportRequestId = request_report()
    GeneratedReportId = get_status_of_request(ReportRequestId)

    dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)

    res_file_name = dir_location + '/%s_active_inventory_report_%s_%s_%s.tsv' % (store, now.hour, now.minute, now.second)
    get_report(GeneratedReportId, res_file_name)
    return res_file_name


def amazon_update_listings(store, res_file_name):

    opsyst_active_listings_rows = Connection.odoo_execute("""
        SELECT LISTING.name FROM product_listing LISTING
        WHERE LISTING.state = 'active' AND LISTING.store_id = %s
    """, [credentials[store]['id']])
    opsyst_active_listings_list = []
    for r in opsyst_active_listings_rows:
        opsyst_active_listings_list.append(r['name'])

    amz_active_listings_list = []

    counter = 1
    with open(res_file_name) as tsv:
        for line in csv.reader(tsv, dialect="excel-tab"):
            if counter == 1:
                counter += 1
                continue
            amz_active_listings_list.append(line[0])

    logging.info('Computing inactive listings for %s.' %store)
    listings_to_deactivate = [item_id for item_id in opsyst_active_listings_list if item_id not in amz_active_listings_list]
    logging.info('Found listings in %s to deactivate: %s' %(store, listings_to_deactivate))
    if listings_to_deactivate:
        logging.info('Deactivating listings for %s.' %store)
        Connection.odoo_execute("""
            UPDATE product_listing SET state = 'ended' WHERE name IN %s
        """, [listings_to_deactivate], commit=True)

    logging.info('Computing unmapped listings for %s.' %store)

    unmapped_or_ended_listings = [item_id for item_id in amz_active_listings_list if item_id not in opsyst_active_listings_list]

    listings_to_reactivate_rows = Connection.odoo_execute("""
        SELECT name FROM product_listing WHERE name IN %s
    """, [unmapped_or_ended_listings])

    listings_to_reactivate = []

    if listings_to_reactivate_rows:
        listings_to_reactivate = [r['name'] for r in listings_to_reactivate_rows]
        Connection.odoo_execute("""
            UPDATE product_listing SET state = 'active' WHERE name IN %s
        """, [listings_to_reactivate], commit=True)

    unmapped_listings = [item_id for item_id in unmapped_or_ended_listings if item_id not in listings_to_reactivate]
    logging.info('Found unmapped listings in %s: %s' %(store, unmapped_listings))
    return unmapped_listings


def sync_listed_products(store_ids):
    logging.info('Syncing listed products.')
    unrecorded_listed_products = Connection.odoo_execute("""
        SELECT PL.product_tmpl_id FROM product_listing PL
        LEFT JOIN product_template PT on PT.id = PL.product_tmpl_id
        WHERE PT.name NOT LIKE '[NOT FOUND]%%' AND PL.state = 'active' AND PL.store_id IN %s AND NOT EXISTS
        (
            SELECT PTL.product_tmpl_id FROM product_template_listed PTL
            WHERE PL.product_tmpl_id = PTL.product_tmpl_id AND PTL.state = 'active'
        )
        GROUP BY product_tmpl_id
    """, [store_ids])

    if unrecorded_listed_products:
        unrecorded_listed_products_len = len(unrecorded_listed_products)
        batches = unrecorded_listed_products_len / 1000
        if unrecorded_listed_products_len % 1000 == 0:
            batches -= 1

        logging.info("To process %s unrecorded listed products in %s batches" %(unrecorded_listed_products_len, batches))
        batch = 0
        while batch <= batches:
            logging.info("Processing batch %s." %(batch))
            batch_list = unrecorded_listed_products[batch * 1000: min(unrecorded_listed_products_len, (batch + 1) * 1000)]

            # First, reactivate product_template_listed that are inactivated
            batch_product_tmpl_ids = []
            for p in batch_list:
                batch_product_tmpl_ids.append(p['product_tmpl_id'])

            batch_product_tmpl_ids_to_reactivate_rows = Connection.odoo_execute("""
                SELECT product_tmpl_id FROM product_template_listed WHERE product_tmpl_id IN %s
            """, [batch_product_tmpl_ids])

            batch_product_tmpl_ids_to_reactivate_list = []
            if batch_product_tmpl_ids_to_reactivate_rows:
                for p in batch_product_tmpl_ids_to_reactivate_rows:
                    batch_product_tmpl_ids_to_reactivate_list.append(p['product_tmpl_id'])
                Connection.odoo_execute("""
                    UPDATE product_template_listed SET state = 'active' WHERE product_tmpl_id IN %s
                """, [batch_product_tmpl_ids_to_reactivate_list], commit=True)

            batch_product_tmpl_ids_to_create = [p for p in batch_product_tmpl_ids if p not in batch_product_tmpl_ids_to_reactivate_list]

            values = ''
            for p in batch_product_tmpl_ids_to_create:
                values += "(%s, 1, 1, now() at time zone 'UTC', now() at time zone 'UTC', 'active')," %p
            if values:
                Connection.odoo_execute("""
                    INSERT INTO product_template_listed (product_tmpl_id, create_uid, write_uid, create_date, write_date, state ) VALUES
                    %s
                """ %values[:-1], commit=True)
            batch += 1

    to_deactivate_listed_products = Connection.odoo_execute("""
        SELECT PTL.product_tmpl_id FROM product_template_listed PTL WHERE PTL.state = 'active' AND NOT EXISTS
        (
	       SELECT PL.product_tmpl_id FROM product_listing PL
           LEFT JOIN product_template PT on PT.id = PL.product_tmpl_id
	       WHERE PT.name NOT LIKE '[NOT FOUND]%%' AND PL.product_tmpl_id = PTL.product_tmpl_id AND PL.state = 'active'
        )
    """)

    if to_deactivate_listed_products:
        to_deactivate_product_tmpl_ids = []
        for p in to_deactivate_listed_products:
            to_deactivate_product_tmpl_ids.append(p['product_tmpl_id'])
        Connection.odoo_execute("""
            UPDATE product_template_listed SET state = 'inactive' WHERE product_tmpl_id IN %s
        """, [to_deactivate_product_tmpl_ids], commit=True)


def sync_components_of_active_listed_products():
    logging.info('Syncing components of active listed products.')

    # Get all components of active listed products that have existing listed product record but already inactive
    inactive_components_rows = Connection.odoo_execute("""
        SELECT PTL.id FROM product_template_listed PTL WHERE PTL.product_tmpl_id IN (
        	SELECT PP.product_tmpl_id FROM product_template_listed PTL
        	LEFT JOIN mrp_bom BOM ON BOM.product_tmpl_id = PTL.product_tmpl_id
        	LEFT JOIN mrp_bom_line BOMLINE ON BOMLINE.bom_id = BOM.id
        	LEFT join product_product PP ON PP.id = BOMLINE.product_id
        	WHERE PTL.state = 'active' AND BOM.id IS NOT NULL
        ) AND PTL.state = 'inactive'
    """)

    inactive_components_list = [r['id'] for r in inactive_components_rows ]

    if inactive_components_list:
        Connection.odoo_execute("""
            UPDATE product_template_listed SET state = 'active' WHERE id IN %s
        """, [inactive_components_list], commit=True)

    # Get all components of active listed products that have no existing listed product record
    new_component_rows = Connection.odoo_execute("""
        SELECT PP.product_tmpl_id FROM product_template_listed PTL
        LEFT JOIN mrp_bom BOM ON BOM.product_tmpl_id = PTL.product_tmpl_id
        LEFT JOIN mrp_bom_line BOMLINE ON BOMLINE.bom_id = BOM.id
        LEFT JOIN product_product PP ON PP.id = BOMLINE.product_id
        LEFT JOIN product_template PT on PT.id = PP.product_tmpl_id
        WHERE PTL.state = 'active' AND BOM.id IS NOT NULL AND NOT EXISTS (
            SELECT PTL.product_tmpl_id FROM product_template_listed PTL
            WHERE PTL.product_tmpl_id = PP.product_tmpl_id
        ) AND PP.product_tmpl_id IS NOT NULL
        GROUP BY PP.product_tmpl_id
    """)

    if new_component_rows:
        new_component_rows_len = len(new_component_rows)
        batches = new_component_rows_len / 1000
        if new_component_rows_len % 1000 == 0:
            batches -= 1

        logging.info("To create %s listed products for components in %s batches" %(new_component_rows_len, batches))
        batch = 0
        while batch <= batches:
            logging.info("Processing batch %s." %(batch))
            batch_list = new_component_rows[batch * 1000: min(new_component_rows_len, (batch + 1) * 1000)]

            values = ''
            for p in batch_list:
                values += "(%s, 1, 1, now() at time zone 'UTC', now() at time zone 'UTC', 'active')," %p['product_tmpl_id']
            if values:
                Connection.odoo_execute("""
                    INSERT INTO product_template_listed (product_tmpl_id, create_uid, write_uid, create_date, write_date, state ) VALUES
                    %s
                """ %values[:-1], commit=True)
            batch += 1


def main():
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    store_ids = []
    for c in credentials:
        store_ids.append(credentials[c]['id'])

    now = datetime.utcnow()

    unmapped_listings = []
    for store in stores:
        if store in credentials:
            if credentials[store]['type'] == 'ebay':
                res_file_name = ebay_download_inventory_report(store, now)
                if not res_file_name:
                    continue
                update_current_prices_of_listings(store, res_file_name)
                unmapped_listings += ebay_update_listings(store, res_file_name)
            elif credentials[store]['type'] == 'amz':
                res_file_name = amazon_download_inventory_report(store, now)
                update_current_prices_of_listings(store, res_file_name)
                unmapped_listings += amazon_update_listings(store, res_file_name)

    if unmapped_listings:
        logging.warning('Unmapped listings found!!! %s' % unmapped_listings)
    sync_listed_products(store_ids)
    sync_components_of_active_listed_products()
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

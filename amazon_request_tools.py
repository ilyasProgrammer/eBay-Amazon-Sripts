# # -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import time
import csv
from lxml import etree
import lmslib
import uuid
import pprint
import logging
import getpass
import simplejson
import sys
import os
import amzConnection
import opsystConnection

current_user = getpass.getuser()
this_module_name = os.path.basename(sys.modules['__main__'].__file__)

thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

STORE_CODE = 'sinister'

logs_path = './logs/'
LOG_FILE = os.path.join(logs_path, '%s_amazon_request_tools_%s.log') % (STORE_CODE, datetime.today().strftime('%Y_%m'))
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
store_model = 'sale.store'
DOMAIN = 'api.ebay.com'
Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
stores = ['sinister']
credentials = Connection.get_store_credentials(stores)


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
        requestreport_params['StartDate'] = (now + timedelta(days=-30)).strftime('%Y-%m-%d'+'T'+'%H:%M:%S')
        requestreport_params['ReportType'] = '_GET_XML_RETURNS_DATA_BY_RETURN_DATE_'
        # requestreport_params['ReportType'] = '_GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE_'
        # requestreport_params['ReportType'] = '_GET_XML_MFN_PRIME_RETURNS_REPORT_'
        response = amz_request.process_amz_request('GET', '/Reports/2009-01-01', requestreport_params)
        ReportRequestId = response['RequestReportResponse']['RequestReportResult']['ReportRequestInfo']['ReportRequestId']['value']
        logging.info('Requested report id %s for %s' % (ReportRequestId, store))
        return ReportRequestId

    def get_status_of_request(ReportRequestId):
        reportrequest_list_params = {}
        reportrequest_list_params['Action'] = 'GetReportRequestList'
        reportrequest_list_params['ReportRequestIdList.Id.1'] = ReportRequestId
        while True:
            response = amz_request.process_amz_request('GET', '/Reports/2009-01-01', reportrequest_list_params)
            status = response['GetReportRequestListResponse']['GetReportRequestListResult']['ReportRequestInfo']['ReportProcessingStatus']['value']
            logging.info('Report status: %s', status)
            if status == '_DONE_':
                GeneratedReportId = response['GetReportRequestListResponse']['GetReportRequestListResult']['ReportRequestInfo']['GeneratedReportId']['value']
                logging.info('Generated report id %s for %s' % (GeneratedReportId, store))
                return GeneratedReportId
            if status == '_CANCELLED_':
                return 'cancelled'
            if status == '_DONE_NO_DATA_':
                return 'no_data'
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

    dir_location = '%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)

    res_file_name = dir_location + '/%s_report_%s_%s_%s.tsv' % (store, now.hour, now.minute, now.second)
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


def delete_listings():
    merchant_identifier = '123412341234'
    xml_content = ""
    xml_content += "<?xml version='1.0' encoding='utf-8'?>\n"
    xml_content += "<AmazonEnvelope xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='amzn-envelope.xsd'>\n"
    xml_content += "<Header>\n"
    xml_content += "<DocumentVersion>1.0</DocumentVersion>\n"
    xml_content += "<MerchantIdentifier>%s</MerchantIdentifier>\n" % merchant_identifier
    xml_content += "</Header>\n"
    xml_content += "<MessageType>OrderFulfillment</MessageType>\n"

    counter = 1
    res_file_name = '/home/ra/Downloads/del.csv'
    with open(res_file_name, 'r') as res_file:
        reader = csv.reader(res_file, delimiter='\t', quotechar='"')
        for row in reader:
            xml_content += "<Message>"
            xml_content += "<MessageID>%s</MessageID>" % counter
            xml_content += "<OperationType>Delete</OperationType>"
            xml_content += "<Product>"
            xml_content += "<SKU>%s</SKU>" % row[0]
            xml_content += "</Product>"
            xml_content += "</Message>\n"
            counter += 1
    xml_content += "</AmazonEnvelope>"
    return xml_content


def main():
    store = 'sinister'
    amz_request = amzConnection.AmzConnection(
            access_id=credentials[store]['access_id'],
            marketplace_id=credentials[store]['marketplace_id'],
            seller_id=credentials[store]['seller_id'],
            secret_key=credentials[store]['secret_key']
        )
    # params = {
    #         'Action': 'GetMatchingProductForId',
    #         'IdList.Id.1': 'LKQ-1110-NEW17-FO1321266',
    #         'IdType': 'SellerSKU',
    #         'MarketplaceId': 'ATVPDKIKX0DER',
    #     }
    #
    # response = amz_request.process_amz_request('POST', '/Products/2011-10-01', params)
    # pass
    now = datetime.now()
    amazon_download_inventory_report(store, now)
    # xml_content = delete_listings()
    # if xml_content:
    #     feed_filename = 'delete_upload_%s_%s_%s.xml' % (now.hour, now.minute, now.second)
    #     feed_file = open(feed_filename, 'wb')
    #     feed_file.write(xml_content)
    #     feed_file.close()
    #

    #
    #     logging.info('Submitting inventory file for %s' % store)
    #     feed_file = open(feed_filename, 'r')
    #     feed_content = feed_file.read()
    #     md5value = amzConnection.get_md5(feed_content)
    #
    #     params = {
    #         'ContentMD5Value': md5value,
    #         'Action': 'SubmitFeed',
    #         'FeedType': '_POST_PRODUCT_DATA_',
    #     }
    #
    #     response = amz_request.process_amz_request('POST', '/Feeds/2009-01-01', params, feed_content)
    #     feed_id = None
    #     if 'FeedSubmissionInfo' in response['SubmitFeedResponse']['SubmitFeedResult']:
    #         feed_info = response['SubmitFeedResponse']['SubmitFeedResult']['FeedSubmissionInfo']
    #         feed_id = feed_info['FeedSubmissionId']['value']
    #         logging.info('Feed submitted %s' % feed_id)
    #
    #     params = {'Action': 'GetFeedSubmissionResult', 'FeedSubmissionId': feed_id}
    #     time.sleep(15)
    #     while True:
    #         try:
    #             response = amz_request.process_amz_request('POST', '/Feeds/2009-01-01', params)
    #             status = response['AmazonEnvelope']['Message']['ProcessingReport']['StatusCode']['value']
    #             if status == 'Complete':
    #                 logging.info('FEED PROCESSED!!!')
    #                 logging.info(pprint.pformat(response))
    #                 break
    #             time.sleep(60)
    #         except Exception as e:
    #             logging.warning(e)
    #             time.sleep(60)
    #     return True
    #


if __name__ == "__main__":
    main()

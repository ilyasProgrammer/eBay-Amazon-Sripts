from datetime import datetime, timedelta
from lxml import etree
from requests import request
from requests.exceptions import HTTPError
from xml.etree.ElementTree import ParseError as XMLError

import base64
import csv
import hashlib
import hmac
import json
import linecache
import lmslib
import logging
import os
import pymssql
import psycopg2
import psycopg2.extras
import re
import simplejson
import sys
import time
import urllib
import utils
import uuid
import zipfile

# PARSE ARGUMENTS AND GET  CONFIGURATION

process_ebay_stores_only = False
test_only = False
no_download = False
no_db_insert = False
no_upload = False
no_build_file = False
sync_product_listings = False
if '--ebay-only' in sys.argv:
    process_ebay_stores_only = True
if '--test-only' in sys.argv:
    test_only = True
if '--no-download' in sys.argv:
    no_download = True
if '--no-db-insert' in sys.argv:
    no_db_insert = True
if '--no-upload' in sys.argv:
    no_upload = True
if '--no-build-file' in sys.argv:
    no_build_file = True
if '--sync-product-listings' in sys.argv:
    sync_product_listings = True

thisdir = os.path.abspath(os.path.dirname( __file__ ))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

odoo_url = config.get('odoo_url')
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
odoo_uid = config.get('odoo_uid')
odoo_password = config.get('odoo_password')
autoplus_db = config.get('autoplus_db')
autoplus_db_host = config.get('autoplus_db_host')
autoplus_db_user = config.get('autoplus_db_user')
autoplus_db_password = config.get('autoplus_db_password')
stores = config.get('stores').split(',')

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    filename=config.get('log_file', ''),
    datefmt='%Y-%m-%d %H:%M:%S')

VENDOR_MFG_LABELS = {
    'PPR': { 'mfg_labels': "('PPR')", 'less_qty': 5 },
    'LKQ': { 'mfg_labels': "('BXLD', 'GMK')", 'less_qty': 5 },
    'PFG': { 'mfg_labels': "('BFHZ', 'REPL', 'STYL', 'NDRE', 'BOLT', 'EVFI')", 'less_qty': 5 }
}

def get_md5(content):
    base64md5 = base64.b64encode(hashlib.md5(content).digest())
    if base64md5[-1] == '\n':
        base64md5 = self.base64md5[0:-1]
    return base64md5

class DataWrapper(object):
    """
        Text wrapper in charge of validating the hash sent by Amazon.
    """
    def __init__(self, data, header):
        self.original = data
        if 'content-md5' in header:
            hash_ = get_md5(self.original)
            if header['content-md5'] != hash_:
                raise MWSError("Wrong Contentlength, maybe amazon error...")

    @property
    def parsed(self):
        return self.original

class DictWrapper(object):
    def __init__(self, xml, rootkey=None):
        self.original = xml
        self._rootkey = rootkey
        self._mydict = utils.xml2dict().fromstring(remove_namespace(xml))
        self._response_dict = self._mydict.get(self._mydict.keys()[0],
                                               self._mydict)

    @property
    def parsed(self):
        if self._rootkey:
            return self._response_dict.get(self._rootkey)
        else:
            return self._response_dict

def remove_namespace(xml):
    regex = re.compile(' xmlns(:ns2)?="[^"]+"|(ns2:)|(xml:)')
    return regex.sub('', xml)

class MWSError(Exception):
    """
        Main MWS Exception class
    """
    # Allows quick access to the response object.
    # Do not rely on this attribute, always check if its not None.
    response = None

def calc_signature(secret_key, verb, uri, domain, request_description):
    sig_data = verb + '\n' + domain.replace('https://', '').lower() + '\n' + uri + '\n' + request_description
    return base64.b64encode(hmac.new(str(secret_key), sig_data, hashlib.sha256).digest())

def process_amz_request(credentials, verb, uri, extra_params=None, feed=None):
    now = datetime.utcnow()
    params = {
        'AWSAccessKeyId': credentials.get('access_id'),
        'MarketplaceId.Id.1': credentials.get('marketplace_id'),
        'SellerId': credentials.get('seller_id'),
        'SignatureVersion': credentials.get('signature_version'),
        'SignatureMethod': credentials.get('signature_method'),
        'Timestamp': now.strftime('%Y-%m-%d'+'T'+'%H:%M:%S'+'.000Z')
    }
    params.update(extra_params)

    request_description = '&'.join(['%s=%s' % (k, urllib.quote(params[k], safe='-_.~').encode('utf-8')) for k in sorted(params)])
    domain = credentials.get('domain')
    secret_key = credentials.get('secret_key')

    signature = calc_signature(secret_key, verb, uri, domain, request_description)

    url = '%s%s?%s&Signature=%s' % (domain, uri, request_description, urllib.quote(signature))
    headers = {
        'User-Agent': 'python-amazon-mws/0.0.1 (Language=Python)'
    }

    if params['Action'] == 'SubmitFeed':
        headers['Host'] = 'mws.amazonservices.com'
        headers['Content-Type'] = 'text/xml'
    if params['Action'] == 'GetReport':
        headers['Host'] = 'mws.amazonservices.com'
        headers['Content-Type'] = 'x-www-form-urlencoded'
    try:
        response = request(verb, url, data=feed, headers=headers)
        response.raise_for_status()
        data = response.content
        try:
            parsed_response = DictWrapper(data, extra_params['Action'] + "Result")
        except XMLError:
            parsed_response = DataWrapper(data, response.headers)
            return data
    except HTTPError, e:
        error = MWSError(str(e.response.text))
        error.response = e.response
        raise error
    parsed_response.response = response
    return parsed_response._mydict

def amazon_download_inventory_report(store, dir_location, credentials):

    def request_report():
        requestreport_params = {}
        requestreport_params['Action'] = 'RequestReport'
        requestreport_params['ReportType'] = '_GET_FLAT_FILE_OPEN_LISTINGS_DATA_'
        response = process_amz_request(credentials, 'GET', '/Reports/2009-01-01', requestreport_params)
        logging.info('Request report response: %s' %json.dumps(response))
        ReportRequestId = response['RequestReportResponse']['RequestReportResult']['ReportRequestInfo']['ReportRequestId']['value']
        return ReportRequestId

    def get_status_of_request(ReportRequestId):
        reportrequest_list_params = {}
        reportrequest_list_params['Action'] = 'GetReportRequestList'
        reportrequest_list_params['ReportRequestIdList.Id.1'] = ReportRequestId
        while True:
            response = process_amz_request(credentials, 'GET', '/Reports/2009-01-01', reportrequest_list_params)
            status = response['GetReportRequestListResponse']['GetReportRequestListResult']['ReportRequestInfo']['ReportProcessingStatus']['value']
            if status == '_DONE_':
                logging.info('Report request status response: %s' %json.dumps(response))
                GeneratedReportId = response['GetReportRequestListResponse']['GetReportRequestListResult']['ReportRequestInfo']['GeneratedReportId']['value']
                return GeneratedReportId
            logging.info('Current Status is %s. Trying again...' %status)
            time.sleep(60)

    def get_report(ReportRequestId, filepath):
        getreport_params = {}
        getreport_params['Action'] = 'GetReport'
        getreport_params['ReportId'] = ReportRequestId
        response = process_amz_request(credentials, 'POST', '/Reports/2009-01-01', getreport_params)
        inv_report_file = open(filepath , 'wb')
        inv_report_file.write(response)
        inv_report_file.close()

    filepath = dir_location + '/' + store +'_active_inventory_report.tsv'
    ReportRequestId = request_report()
    GeneratedReportId = get_status_of_request(ReportRequestId)
    get_report(GeneratedReportId, filepath)
    out_file = open(dir_location + '/' + store +'_active_inventory_report.csv', 'wb')
    counter = 1
    with open(filepath) as tsv:
        for line in csv.reader(tsv, dialect="excel-tab"):
            if counter == 1:
                counter += 1
                continue
            out_file.write('%s;%s;%s;%s\n' %(line[0], line[1], line[2], line[3]))
            counter += 1

def odoo_execute(query, commit=False):
    conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'"
                %(odoo_db, odoo_db_user, odoo_db_host, odoo_db_password)
            )
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute(query)
    if commit:
        conn.commit()
        return True
    else:
        results = cur.fetchall()
        return results

def autoplus_execute(query):
    conn = pymssql.connect('192.169.152.87', 'UsmanRaja', 'Nop@ss786', 'AutoPlus')
    cur = conn.cursor(as_dict=True)
    cur.execute(query)
    results = cur.fetchall()
    return results

def amazon_get_skus_from_odoo(store, dir_location):
    logging.info("Getting %s skus from Odoo" %store)
    results = odoo_execute("""
        SELECT LISTING.name, PRODUCT.part_number
        FROM product_listing LISTING
        LEFT JOIN sale_store STORE on STORE.id = LISTING.store_id
        LEFT JOIN product_template PRODUCT on PRODUCT.id = LISTING.product_tmpl_id
        WHERE STORE.code = '%s'
        """ %store)
    out_file = open(dir_location + '/' + store + '_skus.csv', 'wb')
    for row in results:
        out_file.write('%s;%s\n' %(row['part_number'], row['name']))

def ebay_download_inventory_report(store, dir_location, credentials):
    environment = lmslib.PRODUCTION
    logging.info('Starting download job for %s...' %store)
    download_uuid = uuid.uuid4()
    download_job = lmslib.startDownloadJob(environment, credentials)
    download_job.buildRequest(jobType='ActiveInventoryReport', uuid=download_uuid)
    download_job.sendRequest()
    response, resp_struct = download_job.getResponse()
    if response == 'Success':
        jobId = resp_struct.get('jobId', None )
        logging.info('Successfully created new job %s' %jobId)
    elif response == 'Failure':
        logging.error('startDownloadJob Error[%s]: %s' % (resp_struct.get('errorId',None), resp_struct.get('message', None) ))
        return
    else:
        logging.error("startDownloadJob Error: Something really went wrong here")
        return

    logging.info('Waiting to complete download job')
    downloadFileId = None
    job_status = lmslib.GetJobStatus(environment, credentials)
    job_status.buildRequest(jobId)
    while True:
        response = job_status.sendRequest()
        response, resp_struct = job_status.getResponse()
        if response == 'Success':
            if resp_struct[0].get('jobStatus',None) == 'Completed':
                downloadFileId = resp_struct[0].get( 'fileReferenceId', None )
                logging.info('%s' %resp_struct[0])
                break
            else:
                logging.info("Job is %s complete, trying again in 15 seconds" % resp_struct[0].get('percentComplete', None ))
        elif response == 'Failure':
            logging.error('GetJobStatus Error[%s]: %s' % (resp_struct.get('errorId',None), resp_struct.get('message', None) ))
            return
        else:
            logging.info( "GetJobStatus Error: Something really went wrong here")
            return
        time.sleep(15)

    logging.info("Downloading responses...")
    download_file = lmslib.DownloadFile(environment, credentials)
    download_file.buildRequest( jobId, downloadFileId )
    response = download_file.sendRequest()
    response, resp_struc =  download_file.getResponse()
    if response == 'Success':
        logging.info("Successfully downloaded response!")
    elif response == 'Failure':
        logging.info('DownloadFile [%s]: %s' % (resp_struct.get('errorId',None), resp_struct.get('message', None) ))
        return
    else:
        logging.info("DownloadFile Error: Something really went wrong here")
        return

    ###########################################
    # Unzip and Parse
    ###########################################

    logging.info("Unzipping the download...")
    zip_ref = zipfile.ZipFile('/mnt/vol/misc/data_responses.zip', 'r')
    zip_ref.extractall(dir_location)
    zip_ref.close()

    logging.info("Parsing the report...")
    data_file = open(dir_location + '/%s_report.xml' %jobId, 'r')
    tree = etree.fromstring( data_file.read() )
    result_filename = dir_location + '/' + store + '_active_inventory_report.csv'
    result_file = open(result_filename, 'wb')
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
                    result_file.write('%s;%s;%s;%s\n' %(ItemID, SKU, Price, Quantity))
    logging.info("Report completed...")

def amazon_upload_inventory_to_store(store, dir_location, credentials):
    now = datetime.now()
    logging.info('Creating inventory file for %s' %store)
    merchant_identifier = 'InvFeed_%s_%s_%s_%s_%s_%s' %(now.year, now.month, now.day, now.hour, now.minute, now.second)
    feed_filename = dir_location + '/' + store + '_upload_inventory.xml'
    feed_file = open(feed_filename , 'wb')

    feed_file.write("<?xml version='1.0' encoding='utf-8'?>\n")
    feed_file.write("<AmazonEnvelope xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='amzn-envelope.xsd'>\n")
    feed_file.write("<Header>\n")
    feed_file.write("<DocumentVersion>1.0</DocumentVersion>\n")
    feed_file.write("<MerchantIdentifier>%s</MerchantIdentifier>\n" %merchant_identifier)
    feed_file.write("</Header>\n")
    feed_file.write("<MessageType>OrderFulfillment</MessageType>\n")
    feed_file.write("<PurgeAndReplace>false</PurgeAndReplace>\n")

    listings_to_update_count = odoo_execute("""
        SELECT COUNT(*) as count FROM stock_check_inventory_report
        WHERE store_id = %s AND product_tmpl_id IS NOT NULL
    """ %credentials['id'])

    listings_to_update_count = listings_to_update_count[0]['count']

    counter = 1
    while counter <= listings_to_update_count:
        listings = odoo_execute("""
            SELECT item_id, new_qty, min_handling_time, old_qty FROM stock_check_inventory_report
            WHERE store_id = %s AND product_tmpl_id IS NOT NULL
            ORDER BY id
            LIMIT 1000 OFFSET %s
        """ %(credentials['id'], 1000*(counter / 1000)))

        for listing in listings:
            if not listing['item_id'].startswith('WHCO'):
                xml_body = "<Message>"
                xml_body += "<MessageID>%s</MessageID>" %counter
                xml_body += "<OperationType>Update</OperationType>"
                xml_body += "<Inventory>"
                xml_body += "<SKU>%s</SKU>" %listing['item_id']
                xml_body += "<Quantity>%s</Quantity>" %listing['new_qty']
                if not listing['min_handling_time']:
                    xml_body += "<FulfillmentLatency>3</FulfillmentLatency>"
                xml_body += "</Inventory>"
                xml_body += "</Message>\n"

                feed_file.write(xml_body)
            counter += 1

    feed_file.write("</AmazonEnvelope>")
    feed_file.close()

    if not no_upload:
        logging.info('Submitting inventory file for %s' %store)
        feed_file = open(feed_filename , 'r')
        feed_content = feed_file.read()
        md5value = get_md5(feed_content)

        params = {
            'ContentMD5Value': md5value,
            'Action': 'SubmitFeed',
            'FeedType': '_POST_INVENTORY_AVAILABILITY_DATA_',
            'PurgeAndReplace': 'false'
        }

        response = process_amz_request(credentials, 'POST', '/Feeds/2009-01-01', params, feed_content)

        feed_id = None
        if 'FeedSubmissionInfo' in response['SubmitFeedResponse']['SubmitFeedResult']:
            feed_info = response['SubmitFeedResponse']['SubmitFeedResult']['FeedSubmissionInfo']
            feed_id = feed_info['FeedSubmissionId']['value']
            logging.info('Feed submitted %s' %feed_id)

        params = {
            'Action': 'GetFeedSubmissionResult',
            'FeedSubmissionId': feed_id
        }

        time.sleep(15)

        while True:
            try:
                response = process_amz_request(credentials, 'POST', '/Feeds/2009-01-01', params)
                status = response['AmazonEnvelope']['Message']['ProcessingReport']['StatusCode']['value']
                if status == 'Complete':
                    logging.info('Feed processed!!!')
                    break
                time.sleep(60)
            except:
                logging.info('Feed not yet processed. Trying again...')
                time.sleep(60)

def ebay_upload_inventory_to_store(store, dir_location, credentials):
    logging.info('Creating inventory file for %s' %store)
    result_filename = dir_location + '/' + store + '_active_inventory_report.csv'

    listings_to_update_count = odoo_execute("""
        SELECT COUNT(*) as count FROM stock_check_inventory_report
        WHERE store_id = %s AND product_tmpl_id IS NOT NULL AND old_qty <> new_qty
    """ %credentials['id'])

    listings_to_update_count = listings_to_update_count[0]['count']

    output_file_name = dir_location + '/' + store + '_upload_inventory.xml'
    output_file = open(output_file_name, 'wb')
    output_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
    output_file.write("<BulkDataExchangeRequests>\n")
    output_file.write("<Header>\n")
    output_file.write("<Version>941</Version>\n")
    output_file.write("<SiteID>100</SiteID>\n")
    output_file.write("</Header>\n")

    counter = 1
    while counter <= listings_to_update_count:
        listings = odoo_execute("""
            SELECT item_id,new_qty,old_qty FROM stock_check_inventory_report
            WHERE store_id = %s AND product_tmpl_id IS NOT NULL AND old_qty <> new_qty
            ORDER BY id
            LIMIT 1000 OFFSET %s
        """ %(credentials['id'], 1000*(counter / 1000)))

        inner_total = len(listings)
        inner_counter = 1
        for listing in listings:

            if inner_counter % 4 == 1:
                inner_content = ''

            inner_content += "<InventoryStatus><ItemID>%s</ItemID><Quantity>%s</Quantity></InventoryStatus>\n" %(listing['item_id'], listing['new_qty'])

            if inner_counter % 4 == 0 or inner_counter == inner_total:
                output_file.write("<ReviseInventoryStatusRequest xmlns='urn:ebay:apis:eBLBaseComponents'>\n")
                output_file.write("<Version>941</Version>\n")
                output_file.write(inner_content)
                output_file.write("</ReviseInventoryStatusRequest>\n")

            inner_counter += 1
        counter += 1000

    output_file.write("</BulkDataExchangeRequests>")
    output_file.close()

    if not no_upload:
        logging.info("Uploading inventory file to %s" %store)

        environment = lmslib.PRODUCTION

        logging.info("Creating new UploadJob")

        upload_uuid = uuid.uuid4()
        create_job = lmslib.CreateUploadJob(environment, credentials)
        create_job.buildRequest( 'ReviseInventoryStatus', 'gzip', upload_uuid )
        response = create_job.sendRequest()
        response, resp_struct = create_job.getResponse()

        jobId = None
        fileReferenceId = None
        downloadFileId = None
        filename = output_file_name

        if response == 'Success':
            jobId = resp_struct.get( 'jobId', None )
            fileReferenceId = resp_struct.get( 'fileReferenceId', None )
            if not jobId or not fileReferenceId:
                logging.info("createUploadJob Error: couldn't obtain jobId or fileReferenceId")
                return
            else:
                logging.info("createUploadJob Success!")
                logging.info("%s" %resp_struct)
        elif response == 'Failure':
            logging.info("createUploadJob Error[%s]: %s" % (resp_struct.get('errorId',None), resp_struct.get('message', None) ))
            return
        else:
            logging.info("createUploadJob Error: Something really went wrong here")
            return
        time.sleep(10)

        logging.info("Uploading File")
        upload_job = lmslib.UploadFile(environment, credentials)
        upload_job.buildRequest( jobId, fileReferenceId, filename )
        response = upload_job.sendRequest()
        response, response_struct = upload_job.getResponse()
        if response == 'Failure':
            logging.info("uploadFile Error[%s]" % ( response_struct.get('errorId',None), response_struct.get('message', None )))
            return
        elif response == 'Success':
            logging.info("uploadFile Success")
        else:
            logging.info("uploadFile Error: Something really went wrong here")
            return
        time.sleep(10)

        logging.info("Starting job processing...")
        start_job = lmslib.StartUploadJob(environment, credentials)
        start_job.buildRequest( jobId )
        response = start_job.sendRequest()
        response, response_dict = start_job.getResponse()
        if response == 'Success':
            logging.info("startUploadJob Success!")
            logging.info('%s' %response_struct )
        elif response == 'Failure':
            logging.info("startUploadJob Error[%s]: %s" % (response_struct.get('errorId', None), response_struct.get('message', None  )))
            logging.info('%s' %response_struct )
            return
        else:
            logging.info("startUploadJob Error: Something really went wrong here")
            return
        time.sleep( 10 )

        logging.info("Checking Job Status")
        job_status = lmslib.GetJobStatus(environment, credentials)
        job_status.buildRequest( jobId )

        while True:
            response = job_status.sendRequest()
            response, resp_struct = job_status.getResponse()
            if response == 'Success':
                if resp_struct[0].get('jobStatus',None) == 'Completed':
                    logging.info("Job Finished! Woo hoo!\n")
                    logging.info("%s" %resp_struct[0])
                    downloadFileId = resp_struct[0].get( 'fileReferenceId', None )
                    break
                else:
                    logging.info("Job is %s complete, trying again in 60 seconds" % resp_struct[0].get('percentComplete', None ))
                    logging.info("%s" %resp_struct)
            time.sleep( 60 )

        logging.info("Downloading Responses")
        download_file = lmslib.DownloadFile(environment, credentials)
        download_file.buildRequest( jobId, downloadFileId )
        response = download_file.sendRequest()
        response, resp_struc =  download_file.getResponse()
        if response == 'Success':
            logging.info("Successfully downloaded response!")
            logging.info("%s" %resp_struct)
        elif response == "Failure":
            logging.info("%s" %resp_struct)

        zip_ref = zipfile.ZipFile('/mnt/vol/misc/data_responses.zip', 'r')
        zip_ref.extractall(dir_location)
        zip_ref.close()
        return

def create_repricer_template(store, dir_location, inventory_summary_file_path):
    counter = 1
    out_file_path = '/var/tmp/inv_updates/2017_5_4/sinister_repricer.csv'
    out_file = open(out_file_path, 'wb')
    for line in open('/var/tmp/inv_updates/2017_5_4/sinisterautoparts_2017-05-04_9-59-23.csv'):
        if counter == 1:
            out_file.write('%s' %line)
            counter += 1
        else:
            line_list = line.split(',')
            amz_sku = line_list[0]
            original_min_price = float(line_list[3])
            ase_sku = ''
            for sku_line in open(dir_location + '/' + store + '_skus.csv'):
                if sku_line.split(';')[1][:-1] == amz_sku:
                    ase_sku = sku_line.split(';')[0]
                    break
            if ase_sku:
                if counter % 1000 == 0:
                    logging.info('Processing %s' %counter)
                for inv_line in open(inventory_summary_file_path):
                    if inv_line.split(';')[0] == ase_sku:
                        Cost = 1.1333 * float(inv_line.split(';')[2])
                        if Cost > 1000 or Cost < original_min_price:
                            out_file.write('%s' %line)
                        else:
                            line_list[3] = round(Cost, 2)
                            line_list[4] = 2 * line_list[3]
                            for x in line_list:
                                if x == line_list[-1]:
                                    out_file.write('%s' %x)
                                else:
                                    out_file.write('%s,' %x)
                        break
                else:
                    out_file.write('%s' %line)
            else:
                out_file.write('%s' %line)
            counter += 1
    out_file.close()

def get_store_credentials():
    credentials = {}
    stores_for_query = str(stores).replace('[', '(').replace(']', ')')
    results = odoo_execute("SELECT * FROM sale_store WHERE code in %s" %stores_for_query)
    for res in results:
        credential = {
            'name': res['name'],
            'type': res['site'],
            'id': res['id']
        }
        if res['site'] == 'ebay':
            credential['max_qty'] = res['ebay_max_quantity']
            credential['rate'] = float(res['ebay_rate'])
            credential['last_record_number']= res['ebay_last_record_number']
            credential['domain'] = res['ebay_domain']
            credential['developer_key'] = res['ebay_dev_id']
            credential['application_key'] = res['ebay_app_id']
            credential['certificate_key'] = res['ebay_cert_id']
            credential['auth_token'] = res['ebay_token']
        else:
            credential['max_qty'] = 80
            credential['domain'] = res['amz_domain']
            credential['access_id'] = res['amz_access_id']
            credential['marketplace_id'] = res['amz_marketplace_id']
            credential['seller_id'] = res['amz_seller_id']
            credential['secret_key'] = res['amz_secret_key']
            credential['signature_version'] = res['amz_signature_version']
            credential['signature_method'] = res['amz_signature_method']
        credentials[res['code']] = credential
    return credentials

def clear_stock_check_table(store_ids):
    logging.info('Clearing the stock check table.')
    odoo_execute("DELETE FROM stock_check_inventory_report WHERE store_id IN %s" %store_ids, commit=True)

def upload_to_database(store, dir_location, credentials):
    logging.info('Uploading report from %s to database' %store)
    counter = 1
    report_file_name = dir_location + '/%s_active_inventory_report.csv' %store
    with open(report_file_name) as f:
        for i, l in enumerate(f):
            pass
    total_lines = i + 1

    counter = 1
    while counter <= total_lines:
        inner_counter = 1
        values = ''
        while inner_counter <= 1000 and counter <= total_lines:
            line = linecache.getline(report_file_name, counter)
            list_line = line.split(';')
            if credentials['type'] == 'ebay':
                values += "(%s, '%s', '%s', %s, %s)," %(credentials['id'], list_line[0], list_line[1], list_line[2], list_line[3][:-1].strip('\n'))
            else:
                values += "(%s, '%s', %s, %s)," %(credentials['id'], list_line[0], list_line[2], list_line[3].strip('\n') or 0)
            inner_counter += 1
            counter += 1
        if values and credentials['type'] == 'ebay':
            result = odoo_execute("""
                INSERT INTO stock_check_inventory_report (store_id, item_id, sku, old_price, old_qty ) VALUES
                %s
            """ %values[:-1], commit=True)

        elif values:
            query = """
                INSERT INTO stock_check_inventory_report (store_id, item_id, old_price, old_qty ) VALUES
                %s
            """ %values[:-1]
            result = odoo_execute("""
                INSERT INTO stock_check_inventory_report (store_id, item_id, old_price, old_qty ) VALUES
                %s
            """ %values[:-1], commit=True)

def update_rows_with_product_tmpl_ids(store_ids):
    logging.info('Updating products in rows...')

    # eBay rows have SKUs filled in, while Amazon rows dont

    total_ebay_skus = odoo_execute("""SELECT COUNT(*) as count FROM
            (SELECT sku
             FROM stock_check_inventory_report
             WHERE store_id in %s AND sku IS NOT NULL AND sku NOT LIKE 'X-%%' AND sku NOT LIKE 'MG-%%' AND sku NOT LIKE 'APC-%%'
             GROUP BY sku) as RES
        """ %store_ids)
    total_ebay_skus = int(total_ebay_skus[0]['count'])
    counter = 1
    while counter <= total_ebay_skus:
        skus = odoo_execute("""SELECT sku FROM
            (
                SELECT sku
                FROM stock_check_inventory_report
                WHERE store_id in %s AND sku IS NOT NULL AND sku NOT LIKE 'X-%%' AND sku NOT LIKE 'MG-%%' AND sku NOT LIKE 'APC-%%'
                GROUP BY sku
            ) as RES
            ORDER BY sku OFFSET %s LIMIT 1000
        """ %(store_ids, 1000*(counter / 1000)))

        if skus:
            skus_for_query = '('
            for sku in skus:
                skus_for_query += "'%s'" %sku['sku'] +','
            skus_for_query = skus_for_query[:-1] + ')'

            skus_with_product_tmpl_ids = odoo_execute("""SELECT id, part_number from product_template
                WHERE part_number in %s AND inventory_id IS NOT NULL
            """ %skus_for_query)

            mapping = {}
            for sku in skus_with_product_tmpl_ids:
                mapping[sku['part_number']] = int(sku['id'])

            cases = ''
            for sku in mapping:
                cases += "WHEN sku='%s' THEN %s " %(sku, mapping[sku])

            if cases:
                odoo_execute("""
                    UPDATE stock_check_inventory_report SET product_tmpl_id = (CASE %s END) WHERE sku in %s
                """ %(cases, skus_for_query), commit=True)
            if counter % 10000 == 1:
                logging.info("Processed %s of ebay SKUs" %counter)
        counter += 1000

    total_mg_skus = odoo_execute("""SELECT COUNT(*) as count FROM
            (SELECT sku
             FROM stock_check_inventory_report
             WHERE store_id in %s AND sku LIKE 'MG-%%'
             GROUP BY sku) as RES
        """ %store_ids)
    total_mg_skus = int(total_mg_skus[0]['count'])

    counter = 1
    while counter <= total_mg_skus:
        skus = odoo_execute("""SELECT sku FROM
            (
                SELECT sku
                FROM stock_check_inventory_report
                WHERE store_id in %s AND sku LIKE 'MG-%%'
                GROUP BY sku
            ) as RES
            ORDER BY sku OFFSET %s LIMIT 1000
        """ %(store_ids, 1000*(counter / 1000)))

        if skus:
            skus_for_query = '('
            for sku in skus:
                skus_for_query += "'%s'" %sku['sku'] +','
            skus_for_query = skus_for_query[:-1] + ')'

            skus_mg_mappings = autoplus_execute("""SELECT INV.PartNo as LADPartNo, INV2.PartNo as MGPartNo
                FROM Inventory INV
                LEFT JOIN InventoryAlt ALT on ALT.InventoryIdAlt = INV.InventoryId
                LEFT JOIN Inventory INV2 on ALT.InventoryId = INV2.InventoryId
                WHERE INV2.PartNo IN %s AND INV2.MfgID = 40 and INV.MfgID = 1
            """ %skus_for_query)

            mapping = {}
            lad_skus_for_query = '('
            for sku in skus_mg_mappings:
                if sku['LADPartNo']:
                    mapping[sku['LADPartNo']] = sku['MGPartNo']
                    lad_skus_for_query += "'%s'" %sku['LADPartNo'] +','
            lad_skus_for_query = lad_skus_for_query[:-1] + ')'

            skus_with_product_tmpl_ids = odoo_execute("""SELECT id, part_number from product_template
                WHERE part_number in %s AND inventory_id IS NOT NULL
            """ %lad_skus_for_query)

            lad_sku_mapping = {}
            for sku in skus_with_product_tmpl_ids:
                if sku['part_number']:
                    lad_sku_mapping[sku['part_number']] = sku['id']

            cases = ''
            for sku in lad_sku_mapping:
                if sku in mapping:
                    cases += "WHEN sku='%s' THEN %s " %(mapping[sku], lad_sku_mapping[sku])

            if cases:
                odoo_execute("""
                    UPDATE stock_check_inventory_report SET product_tmpl_id = (CASE %s END) WHERE sku in %s
                """ %(cases, skus_for_query), commit=True)
            if counter % 10000 == 1:
                logging.info("Processed %s of Magnifiek SKUs" %counter)
        counter += 1000

    total_apc_skus = odoo_execute("""SELECT COUNT(*) as count FROM
            (SELECT sku
             FROM stock_check_inventory_report
             WHERE store_id in %s AND sku LIKE 'APC-%%'
             GROUP BY sku) as RES
        """ %store_ids)
    total_apc_skus = int(total_apc_skus[0]['count'])

    counter = 1
    while counter <= total_apc_skus:
        skus = odoo_execute("""SELECT sku FROM
            (
                SELECT sku
                FROM stock_check_inventory_report
                WHERE store_id in %s AND sku LIKE 'APC-%%'
                GROUP BY sku
            ) as RES
            ORDER BY sku OFFSET %s LIMIT 1000
        """ %(store_ids, 1000*(counter / 1000)))

        if skus:
            skus_for_query = '('
            for sku in skus:
                skus_for_query += "'%s'" %sku['sku'] +','
            skus_for_query = skus_for_query[:-1] + ')'

            skus_apc_mappings = autoplus_execute("""SELECT INV.PartNo as LADPartNo, INV2.PartNo as APCPartNo
                FROM Inventory INV
                LEFT JOIN InventoryAlt ALT on ALT.InventoryIdAlt = INV.InventoryId
                LEFT JOIN Inventory INV2 on ALT.InventoryId = INV2.InventoryId
                WHERE INV2.PartNo IN %s AND INV2.MfgID = 42 and INV.MfgID = 1
            """ %skus_for_query)

            mapping = {}
            lad_skus_for_query = '('
            for sku in skus_apc_mappings:
                if sku['LADPartNo']:
                    mapping[sku['LADPartNo']] = sku['APCPartNo']
                    lad_skus_for_query += "'%s'" %sku['LADPartNo'] +','
            lad_skus_for_query = lad_skus_for_query[:-1] + ')'

            skus_with_product_tmpl_ids = odoo_execute("""SELECT id, part_number from product_template
                WHERE part_number in %s AND inventory_id IS NOT NULL
            """ %lad_skus_for_query)

            lad_sku_mapping = {}
            for sku in skus_with_product_tmpl_ids:
                if sku['part_number']:
                    lad_sku_mapping[sku['part_number']] = sku['id']

            cases = ''
            for sku in lad_sku_mapping:
                if sku in mapping:
                    cases += "WHEN sku='%s' THEN %s " %(mapping[sku], lad_sku_mapping[sku])

            if cases:
                odoo_execute("""
                    UPDATE stock_check_inventory_report SET product_tmpl_id = (CASE %s END) WHERE sku in %s
                """ %(cases, skus_for_query), commit=True)
            if counter % 10000 == 1:
                logging.info("Processed %s of APC SKUs" %counter)
        counter += 1000

    total_amz_skus = odoo_execute("""SELECT COUNT(*) as count FROM
            (SELECT item_id
             FROM stock_check_inventory_report
             WHERE store_id in %s AND sku IS NULL) as RES
        """ %store_ids)
    total_amz_skus = int(total_amz_skus[0]['count'])
    counter = 1
    while counter <= total_amz_skus:
        skus = odoo_execute("""SELECT item_id FROM
            (
                SELECT item_id
                FROM stock_check_inventory_report
                WHERE store_id in %s AND sku IS NULL
            ) as RES
            ORDER BY item_id OFFSET %s LIMIT 1000
        """ %(store_ids, 1000*(counter / 1000)))

        if skus:
            skus_for_query = '('
            for sku in skus:
                skus_for_query += "'%s'" %sku['item_id'] +','
            skus_for_query = skus_for_query[:-1] + ')'

            skus_with_product_tmpl_ids = odoo_execute("""SELECT product_tmpl_id, name from product_listing
                WHERE name in %s
            """ %skus_for_query)

            mapping = {}
            for sku in skus_with_product_tmpl_ids:
                if sku['product_tmpl_id']:
                    mapping[sku['name']] = int(sku['product_tmpl_id'])
            cases = ''
            for sku in mapping:
                cases += "WHEN item_id='%s' THEN %s " %(sku, mapping[sku])
            if cases:
                odoo_execute("""
                    UPDATE stock_check_inventory_report SET product_tmpl_id = (CASE %s END) WHERE item_id in %s
                """ %(cases, skus_for_query), commit=True)
            if counter % 10000 == 1:
                logging.info("Processed %s of Amazon SKUs" %counter)
        counter += 1000

def update_rows_if_kit(store_ids):
    logging.info('Identifying kits')
    kits_updated = odoo_execute("""
        UPDATE stock_check_inventory_report
        SET is_a_kit = True WHERE store_id in %s AND product_tmpl_id in (
            SELECT product_tmpl_id FROM mrp_bom
        )
    """ %store_ids, commit=True)

def get_vendor_availability(store_ids):
    logging.info('Getting vendor availability')

    total_products = odoo_execute("""SELECT COUNT(*) as count FROM
        (SELECT product_tmpl_id
         FROM stock_check_inventory_report
         WHERE store_id in %s AND product_tmpl_id IS NOT NULL AND is_a_kit IS NULL GROUP BY product_tmpl_id) as RES
    """ %store_ids)
    total_products = int(total_products[0]['count'])

    counter = 1
    while counter <= total_products:
        rows = odoo_execute("""SELECT PT.part_number, PT.id FROM product_template PT WHERE PT.id IN
            (
                SELECT RES.product_tmpl_id FROM
                    (SELECT product_tmpl_id
                     FROM stock_check_inventory_report
                     WHERE store_id in %s AND product_tmpl_id IS NOT NULL AND is_a_kit IS NULL
                     GROUP BY product_tmpl_id) as RES
                ORDER BY product_tmpl_id OFFSET %s LIMIT 1000
            )
        """ %(store_ids, 1000*(counter / 1000)))

        if rows:
            mapping = {}
            skus_for_query, product_tmpl_ids_for_query = '(', '('
            for row in rows:
                mapping[row['part_number']] = row['id']
                skus_for_query += "'%s'" %row['part_number'] +','
                product_tmpl_ids_for_query += "%s" %row['id'] +','
            skus_for_query = skus_for_query[:-1] + ')'
            product_tmpl_ids_for_query = product_tmpl_ids_for_query[:-1] + ')'

            subquery = ''
            vendor_counter = 1
            for vendor in VENDOR_MFG_LABELS:
                subquery += """
                    (SELECT INV2.PartNo as part_number, INV.QtyOnHand as qty, PR.Cost as cost
                    FROM InventoryAlt ALT
                    LEFT JOIN Inventory INV on ALT.InventoryIDAlt = INV.InventoryID
                    LEFT JOIN Inventory INV2 on ALT.InventoryID = INV2.InventoryID
                    LEFT JOIN InventoryMiscPrCur PR ON INV.InventoryID = PR.InventoryID
                    LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
                    WHERE MFG.MfgCode IN %s AND INV.QtyOnHand > %s AND INV2.PartNo IN %s)

                    UNION

                    (SELECT INV.PartNo as part_number, INV.QtyOnHand as qty, PR.Cost as cost
                    FROM Inventory INV
                    LEFT JOIN InventoryMiscPrCur PR ON INV.InventoryID = PR.InventoryID
                    LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
                    WHERE MFG.MfgCode IN %s AND INV.QtyOnHand > %s AND INV.PartNo IN %s)
                """ %(VENDOR_MFG_LABELS[vendor]['mfg_labels'], VENDOR_MFG_LABELS[vendor]['less_qty'], skus_for_query,
                      VENDOR_MFG_LABELS[vendor]['mfg_labels'], VENDOR_MFG_LABELS[vendor]['less_qty'], skus_for_query)
                if vendor_counter < len(VENDOR_MFG_LABELS):
                    subquery += 'UNION'
                vendor_counter += 1

            vendor_availabilty = autoplus_execute("""
                SELECT RES.part_number, SUM(RES.qty) as qty, MIN(RES.cost) as cost FROM
                (
                    %s
                ) as RES GROUP BY RES.part_number
            """ %subquery
            )

            qty_cases, cost_cases = '', ''
            for row in vendor_availabilty:
                qty = int(row['qty']) if row['qty'] else 0
                cost = float(row['cost']) if row['cost'] else 0.0
                qty_cases += "WHEN product_tmpl_id= %s THEN %s " %(mapping[row['part_number']], qty)
                cost_cases += "WHEN product_tmpl_id= %s THEN %s " %(mapping[row['part_number']], cost)

            if qty_cases and cost_cases:
                odoo_execute("""
                    UPDATE stock_check_inventory_report SET vendor_cost = (CASE %s END), vendor_availability = (CASE %s END) WHERE product_tmpl_id in %s
                """ %(cost_cases, qty_cases, product_tmpl_ids_for_query), commit=True)
        if counter % 10000 == 1:
            logging.info("Obtained vendor availability of %s products" %counter)
        counter += 1000

def get_wh_availability(store_ids):
    logging.info('Getting WH availability')

    wh_products = odoo_execute("""
        SELECT TEMPLATE.id, RES2.qty, RES1.cost
        FROM
            (SELECT QUANT.product_id, SUM(QUANT.qty) as qty, SUM(QUANT.qty * QUANT.cost) / SUM(QUANT.qty) as cost
            FROM stock_quant QUANT
            LEFT JOIN stock_location LOC on QUANT.location_id = LOC.id
            WHERE LOC.usage = 'internal' AND QUANT.cost > 0 AND QUANT.qty > 0 AND QUANT.reservation_id IS NULL
            GROUP BY QUANT.product_id) as RES1
        LEFT JOIN
            (SELECT QUANT.product_id, SUM(QUANT.qty) as qty
            FROM stock_quant QUANT
            LEFT JOIN stock_location LOC on QUANT.location_id = LOC.id
            WHERE LOC.usage = 'internal' AND QUANT.cost > 0 AND QUANT.qty > 0 AND QUANT.reservation_id IS NULL
            GROUP BY QUANT.product_id) as RES2 on RES1.product_id = RES2.product_id
        LEFT JOIN product_product PRODUCT on PRODUCT.id = RES1.product_id
        LEFT JOIN product_template TEMPLATE on TEMPLATE.id = PRODUCT.product_tmpl_id
    """)

    wh_products_count = len(wh_products)

    product_tmpl_ids_for_query = '('
    product_tmpl_ids_with_min_handling_time_for_query = '('
    qty_cases, cost_cases = '', ''
    for row in wh_products:
        product_tmpl_ids_for_query += "%s" %row['id'] +','
        qty = int(row['qty']) if row['qty'] else 0
        qty_cases += "WHEN product_tmpl_id= %s THEN %s " %(row['id'], qty)
        if qty >= 1:
            product_tmpl_ids_with_min_handling_time_for_query += "%s" %row['id'] +','
        cost_cases += "WHEN product_tmpl_id= %s THEN %s " %(row['id'], float(row['cost']))
    product_tmpl_ids_for_query = product_tmpl_ids_for_query[:-1] + ')'
    product_tmpl_ids_with_min_handling_time_for_query = product_tmpl_ids_with_min_handling_time_for_query[:-1] + ')'

    if qty_cases and cost_cases:
        odoo_execute("""
            UPDATE stock_check_inventory_report SET wh_cost = (CASE %s END), wh_availability = (CASE %s END), min_handling_time = (CASE WHEN product_tmpl_id IN %s THEN True END) WHERE product_tmpl_id in %s
                """ %(cost_cases, qty_cases, product_tmpl_ids_with_min_handling_time_for_query, product_tmpl_ids_for_query), commit=True)

def get_kits_availability(store_ids):
    logging.info('Getting availability of kits')
    total_kits = odoo_execute("""SELECT COUNT(*) as count FROM
        (SELECT product_tmpl_id
         FROM stock_check_inventory_report
         WHERE store_id in %s AND product_tmpl_id IS NOT NULL AND is_a_kit = True GROUP BY product_tmpl_id) as RES
    """ %store_ids)
    total_kits = int(total_kits[0]['count'])

    counter = 1
    while counter <= total_kits:
        # Get kits form stock_check_inventory_report
        rows = odoo_execute("""SELECT PT.id, PT.part_number FROM product_template PT WHERE PT.id IN
            (
                SELECT RES.product_tmpl_id FROM
                    (SELECT product_tmpl_id
                     FROM stock_check_inventory_report
                     WHERE store_id in %s AND product_tmpl_id IS NOT NULL AND is_a_kit = True
                     GROUP BY product_tmpl_id) as RES
                ORDER BY product_tmpl_id OFFSET %s LIMIT 500
            )
        """ %(store_ids, 500*(counter / 500)))
        # Parse the product_tmpl_ids of kits

        mapping = {}
        product_tmpl_ids_for_query, skus_for_query = '(', '('
        for row in rows:
            skus_for_query += "'%s'" %row['part_number'] + ','
            product_tmpl_ids_for_query += "%s" %row['id'] +','
            mapping[row['part_number']] = row['id']
        product_tmpl_ids_for_query = product_tmpl_ids_for_query[:-1] + ')'
        skus_for_query = skus_for_query[:-1] + ')'

        kits_wh_availability = odoo_execute("""
            SELECT PT.id, MIN(CASE WHEN RES.qty IS NULL THEN 0 ELSE RES.qty END) as qty
            FROM mrp_bom_line BOMLINE
            LEFT JOIN mrp_bom BOM on BOMLINE.bom_id = BOM.id
            LEFT JOIN product_template PT on PT.id = BOM.product_tmpl_id
            LEFT JOIN
                (SELECT QUANT.product_id, SUM(QUANT.qty) as qty
                FROM stock_quant QUANT
                LEFT JOIN product_product PRODUCT on PRODUCT.id = QUANT.product_id
                LEFT JOIN stock_location LOC on QUANT.location_id = LOC.id
                WHERE LOC.usage = 'internal' AND QUANT.qty > 0
                GROUP BY QUANT.product_id) as RES
            ON RES.product_id = BOMLINE.product_id
            WHERE PT.id in %s
            GROUP BY PT.id""" %(product_tmpl_ids_for_query)
        )

        wh_qty_cases = ''
        product_tmpl_ids_with_min_handling_time_for_query = '('
        for row in kits_wh_availability:
            qty = int(row['qty']) if row['qty'] else 0
            wh_qty_cases += "WHEN product_tmpl_id= %s THEN %s " %(row['id'], qty)
            if qty >= 2:
                product_tmpl_ids_with_min_handling_time_for_query += "%s" %row['id'] +','
        product_tmpl_ids_with_min_handling_time_for_query = product_tmpl_ids_with_min_handling_time_for_query[:-1] + ')'

        if wh_qty_cases:
            odoo_execute("""
                UPDATE stock_check_inventory_report SET wh_availability = (CASE %s END), min_handling_time = (CASE WHEN product_tmpl_id IN %s THEN True END) WHERE product_tmpl_id in %s
                    """ %(wh_qty_cases, product_tmpl_ids_with_min_handling_time_for_query, product_tmpl_ids_for_query), commit=True)

        subquery = ''
        vendor_counter = 1
        for vendor in VENDOR_MFG_LABELS:
            subquery += """
                (SELECT INV2.PartNo as part_number, INV.QtyOnHand as qty, PR.Cost as cost
                FROM InventoryAlt ALT
                LEFT JOIN Inventory INV on ALT.InventoryIDAlt = INV.InventoryID
                LEFT JOIN Inventory INV2 on ALT.InventoryID = INV2.InventoryID
                LEFT JOIN InventoryMiscPrCur PR ON INV.InventoryID = PR.InventoryID
                LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
                WHERE MFG.MfgCode IN %s AND INV.QtyOnHand > %s)

                UNION

                (SELECT INV.PartNo as part_number, INV.QtyOnHand as qty, PR.Cost as cost
                FROM Inventory INV
                LEFT JOIN InventoryMiscPrCur PR ON INV.InventoryID = PR.InventoryID
                LEFT JOIN Mfg MFG on MFG.MfgID = INV.MfgID
                WHERE MFG.MfgCode IN %s AND INV.QtyOnHand > %s)
            """ %(VENDOR_MFG_LABELS[vendor]['mfg_labels'], VENDOR_MFG_LABELS[vendor]['less_qty'],
                  VENDOR_MFG_LABELS[vendor]['mfg_labels'], VENDOR_MFG_LABELS[vendor]['less_qty'])
            if vendor_counter < len(VENDOR_MFG_LABELS):
                subquery += 'UNION'
            vendor_counter += 1

        kits_vendor_availability = autoplus_execute("""
            SELECT INVKIT.PartNo as part_number, MIN(CASE WHEN RES2.qty IS NULL THEN 0 ELSE RES2.qty END) as qty, SUM(RES2.cost) as cost
            FROM InventoryPiesKit KIT
            LEFT JOIN Inventory INV on INV.PartNo = KIT.PartNo
            LEFT JOIN Inventory INVKIT on  INVKIT.InventoryID = KIT.InventoryID
            LEFT JOIN (
                SELECT RES.part_number, SUM(RES.qty) as qty, MIN(RES.cost) as cost FROM
                (
                    %s
                ) as RES GROUP BY RES.part_number
            ) as RES2 ON RES2.part_number = INV.PartNo
            WHERE INVKIT.PartNo IN %s
            GROUP BY INVKIT.PartNo""" %(subquery, skus_for_query))

        vendor_qty_cases = ''
        for row in kits_vendor_availability:
            qty = int(row['qty']) if row['qty'] else 0
            vendor_qty_cases += "WHEN product_tmpl_id= %s THEN %s " %(mapping[row['part_number']], qty)

        if vendor_qty_cases:
            odoo_execute("""
                UPDATE stock_check_inventory_report SET vendor_availability = (CASE %s END) WHERE product_tmpl_id in %s
            """ %(vendor_qty_cases, product_tmpl_ids_for_query), commit=True)

        counter += 500

    # Process availability for kits whose components are not entirely from warehouse or vendor

    total_kits_unavailable =  odoo_execute("""SELECT COUNT(*) as count FROM
        (SELECT product_tmpl_id
         FROM stock_check_inventory_report
         WHERE store_id in %s AND product_tmpl_id IS NOT NULL AND is_a_kit = True AND wh_availability = 0 AND vendor_availability = 0 GROUP BY product_tmpl_id) as RES
    """ %store_ids)

    total_kits_unavailable = int(total_kits_unavailable[0]['count'])
    counter = 1
    while counter <= total_kits_unavailable:
        rows = odoo_execute("""SELECT PT.id, PT.part_number FROM product_template PT WHERE PT.id IN
            (
                SELECT RES.product_tmpl_id FROM
                    (SELECT product_tmpl_id
                     FROM stock_check_inventory_report
                     WHERE store_id in %s AND product_tmpl_id IS NOT NULL AND is_a_kit = True
                     AND wh_availability = 0 AND vendor_availability = 0
                     GROUP BY product_tmpl_id) as RES
                ORDER BY product_tmpl_id OFFSET %s LIMIT 500
            )
        """ %(store_ids, 500*(counter / 500)))

        product_tmpl_ids_for_query = '('
        for row in rows:
            product_tmpl_ids_for_query += "%s" %row['id'] +','
        product_tmpl_ids_for_query = product_tmpl_ids_for_query[:-1] + ')'

        kits_vendor_availability = odoo_execute("""SELECT PT.id,
            MIN(CASE WHEN RES.qty IS NULL THEN 0 ELSE RES.qty END) as qty
            FROM mrp_bom_line BOMLINE
            LEFT JOIN product_product PPCOMP on BOMLINE.product_id = PPCOMP.id
            LEFT JOIN mrp_bom BOM on BOMLINE.bom_id = BOM.id
            LEFT JOIN product_template PT on PT.id = BOM.product_tmpl_id
            LEFT JOIN
                (SELECT SC.product_tmpl_id, MAX(SC.wh_availability + SC.vendor_availability) as qty
                FROM stock_check_inventory_report SC
                GROUP BY SC.product_tmpl_id) as RES
            ON RES.product_tmpl_id = PPCOMP.product_tmpl_id
            WHERE PT.id in %s
            GROUP BY PT.id""" %product_tmpl_ids_for_query)

	vendor_qty_cases = ''
        for row in kits_vendor_availability:
            qty = int(row['qty']) if row['qty'] else 0
            vendor_qty_cases += "WHEN product_tmpl_id=%s THEN %s " %(row['id'], qty)
        print vendor_qty_cases
        if vendor_qty_cases:
            query = """
                UPDATE stock_check_inventory_report SET vendor_availability = (CASE %s END) WHERE product_tmpl_id in %s
            """ %(vendor_qty_cases, product_tmpl_ids_for_query)
            print query
            odoo_execute(query, commit=True)
        counter += 500

def compute_new_price_and_qty(store_ids, credentials):
    odoo_execute("""UPDATE stock_check_inventory_report SET vendor_availability = 0
            WHERE store_id in %s AND vendor_availability IS NULL
        """ %store_ids, commit=True)
    odoo_execute("""UPDATE stock_check_inventory_report SET wh_availability = 0
            WHERE store_id in %s AND wh_availability IS NULL
        """ %store_ids, commit=True)
    for c in credentials:
        odoo_execute("""
            UPDATE stock_check_inventory_report SET new_qty =
                (CASE
                    WHEN (vendor_availability + wh_availability) >= %s THEN %s \
                    WHEN (vendor_availability + wh_availability) > 0 THEN (vendor_availability + wh_availability)
                    ELSE 0
                END)
            WHERE store_id = %s
        """ %(credentials[c]['max_qty'], credentials[c]['max_qty'], credentials[c]['id']), commit=True)

def main():
    now = datetime.utcnow()

    credentials = get_store_credentials()
    store_ids = []
    for c in credentials:
        store_ids.append(credentials[c]['id'])
    store_ids = str(store_ids).replace('[', '(').replace(']',')')

    if test_only:
        dir_location = '/Users/ajporlante/auto/inv_updates/%s_%s_%s' %(now.year, now.month, now.day)
    else:
        dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' %(now.year, now.month, now.day)

    if not os.path.exists(dir_location):
        os.makedirs(dir_location)

    if not no_download:
        for store in stores:
            if store in credentials:
                if credentials[store]['type'] == 'ebay':
                    ebay_download_inventory_report(store, dir_location, credentials[store])
                elif credentials[store]['type'] == 'amz':
                    amazon_download_inventory_report(store, dir_location, credentials[store])

    #if sync_product_listings:
    #    if store in credentials:
    #        if credentials[store]['type'] == 'ebay':
    #            ebay_upload_inventory_to_store(store, dir_location, credentials[store])

    if not no_db_insert:
        clear_stock_check_table(store_ids)
        for store in stores:
            if store in credentials:
                upload_to_database(store, dir_location, credentials[store])
        update_rows_with_product_tmpl_ids(store_ids)
        update_rows_if_kit(store_ids)
        get_vendor_availability(store_ids)
        get_wh_availability(store_ids)
        get_kits_availability(store_ids)
        compute_new_price_and_qty(store_ids, credentials)

    for store in stores:
        if store in credentials:
            if credentials[store]['type'] == 'ebay':
                ebay_upload_inventory_to_store(store, dir_location, credentials[store])
            if credentials[store]['type'] == 'amz':
                amazon_upload_inventory_to_store(store, dir_location, credentials[store])

if __name__ == "__main__":
    main()

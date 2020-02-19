from datetime import datetime
from lxml import etree
import opsystConnection
import lmslib
import logging
import simplejson
import os
import sys
import time
import uuid
import xmlrpclib
import zipfile
import slack
import rpc


logging.captureWarnings(True)
logging.getLogger('requests').setLevel(logging.ERROR)
thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)
logs_path = config.get('logs_path')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')
this_module_name = os.path.basename(sys.modules['__main__'].__file__)
dir_location = '/var/tmp/tracking'
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
odoo_password = config.get('odoo_password')
# models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(odoo_db_host))
stores = config.get('stores').split(',')
Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db,
                                               odoo_db_host=odoo_db_host,
                                               odoo_db_user=odoo_db_user,
                                               odoo_db_password=odoo_password)
credentials = Connection.get_store_credentials(stores)
LOG_FILE = os.path.join(logs_path + 'tebay_trackings_%s.log') % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)

slack_bot_token = "xoxb-312554961652-uSmliU84rFhnUSBq9YdKh6lS"
slack_channel_id = "C9DQU2ZV3"
slack_cron_info_channel_id = "CGNGTRB7X"


def ebay_send_tracking_info(store, dir_location):
    logging.info('Processing tracking upload for %s...' % store)
    now = datetime.utcnow()
    filename = dir_location + '/' + store + '_upload_tracking.xml'
    shipped_time = now.strftime('%Y-%m-%d' + 'T' + '%H:%M:%S' + '.000Z')
    file = open(filename, 'wb')
    file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
    file.write("<BulkDataExchangeRequests>\n")
    file.write("<Header>\n")
    file.write("<Version>1</Version>\n")
    file.write("<SiteID>100</SiteID>\n")
    file.write("</Header>\n")

    picking_ids = rpc.read('stock.picking',
                           [[['tracking_number', '!=', False],
                             ['store_id.code', '=', store],
                             ['store_notified', '=', False],
                             ['write_date', '>=', '2018-01-01 00:00:00']
                             ]]
                           )
    # logging.info("Pickings: %s", picking_ids)
    has_upload = False
    order_line_list = []

    for picking in picking_ids:
        if not picking['sale_id']:
            continue
        order_lines = rpc.read('sale.order.line',[[['order_id', '=', picking['sale_id'][0]]]])
        order_id = rpc.read('sale.order', [[['id', '=', picking['sale_id'][0]]]])
        if picking['carrier_id']:
            for order_line in order_lines:
                if order_line['web_orderline_id'] and order_line['web_orderline_id'] not in order_line_list:
                    file.write("<SetShipmentTrackingInfoRequest xmlns='urn:ebay:apis:eBLBaseComponents'>")
                    file.write("<OrderID>%s</OrderID>" % order_id[0]['web_order_id'])
                    file.write("<OrderLineItemID>%s</OrderLineItemID>" % order_line['web_orderline_id'])
                    file.write("<Shipment>")
                    file.write("<ShippedTime>%s</ShippedTime>" % shipped_time)
                    file.write("<ShipmentTrackingDetails>")
                    file.write("<ShippingCarrierUsed>%s</ShippingCarrierUsed>" % picking['carrier_id'][1])
                    file.write("<ShipmentTrackingNumber>%s</ShipmentTrackingNumber>" % picking['tracking_number'])
                    file.write("</ShipmentTrackingDetails>")
                    file.write("</Shipment>")
                    file.write("</SetShipmentTrackingInfoRequest>\n")
                    order_line_list.append(order_line['web_orderline_id'])
                    has_upload = True
    file.write("</BulkDataExchangeRequests>")
    file.close()

    if has_upload:
        credentials = config['stores'][store]

        if config['stores'][store]['domain'] == 'api.ebay.com':
            environment = lmslib.PRODUCTION
        else:
            environment = lmslib.SANDBOX

        logging.info('Creating upload job')
        build_uuid = uuid.uuid4()
        create_job = lmslib.CreateUploadJob(environment, credentials)
        create_job.buildRequest('SetShipmentTrackingInfo', 'gzip', build_uuid)
        create_job.sendRequest()
        response, resp_struct = create_job.getResponse()
        if response == 'Success':
            jobId = resp_struct.get('jobId', None)
            fileReferenceId = resp_struct.get('fileReferenceId', None)
            logging.info('Job created: job id %s, file reference id %s' % (jobId, fileReferenceId))
        elif response == 'Failure':
            logging.error('%s' % resp_struct.get('message', None))
            return

        time.sleep(10)

        logging.info('Uploading file')
        upload_file = lmslib.UploadFile(environment, credentials)
        upload_file.buildRequest(jobId, fileReferenceId, filename)
        upload_file.sendRequest()
        response, response_dict = upload_file.getResponse()
        if response == 'Success':
            logging.info('File uploaded')
        else:
            logging.error('%s' % resp_struct.get('message', None))
            return

        time.sleep(10)

        logging.info('Starting upload job')
        start_upload_job = lmslib.StartUploadJob(environment, credentials)
        start_upload_job.buildRequest(jobId)
        start_upload_job.sendRequest()
        response, response_dict = start_upload_job.getResponse()
        if response == 'Success':
            logging.info('Upload job started.')
        else:
            logging.error('%s' % resp_struct.get('message', None))
            return

        time.sleep(10)

        logging.info('Checking job status.')
        job_status = lmslib.GetJobStatus(environment, credentials)
        job_status.buildRequest(jobId)
        while True:
            response = job_status.sendRequest()
            response, resp_struct = job_status.getResponse()
            if response == 'Success':
                if resp_struct[0].get('jobStatus', None) == 'Completed':
                    downloadFileId = resp_struct[0].get('fileReferenceId', None)
                    logging.info('Job finished. Download file Id %s' % downloadFileId)
                    break
                else:
                    logging.info("Job is %s complete, trying again in 20 seconds" % resp_struct[0].get('percentComplete', None))
            time.sleep(20)

        logging.info('Downloading file.')
        download_file = lmslib.DownloadFile(environment, credentials)
        download_file.buildRequest(jobId, downloadFileId)
        download_file.sendRequest()
        response, resp_struc = download_file.getResponse()
        if response == 'Success':
            logging.info('Successfully downloaded response.')
        elif response == "Failure":
            logging.error('%s' % resp_struct.get('message', None))
            slack.notify_slack(this_module_name, "Error: %s" % resp_struct.get('message', None), slack_bot_token=slack_bot_token,  slack_cron_info_channel_id=slack_channel_id)
            return

        logging.info('Unzipping the download.')
        zip_ref = zipfile.ZipFile('/var/tmp/data_responses.zip', 'r')
        zip_ref.extractall(dir_location)
        zip_ref.close()

        logging.info('Parsing the response.')
        data_file = open(dir_location + '/%s_responses.xml' % jobId, 'r')
        tree = etree.fromstring(data_file.read())

        order_line_item_ids = []
        for node in tree.getchildren():
            if node.tag.endswith('SetShipmentTrackingInfoResponse'):
                Ack = ''
                Error = False
                for child in node.getchildren():
                    if child.tag.endswith('Ack'):
                        Ack = child.text
                    if child.tag.endswith('OrderLineItemID'):
                        OrderLineItemID = child.text
                    if child.tag.endswith('Error') or child.tag.endswith('Errors'):
                        for child2 in child.getchildren():
                            if child2.tag.endswith('ShortDescription') or child2.tag.endswith('ShortMessage'):
                                Error = child2.text

                if Ack == 'Success':
                    order_line_item_ids.append(OrderLineItemID)
                elif Ack == 'Failure' and (Error == '<![CDATA[Tracking number(s) already exists.]]>' or Error == 'Tracking number(s) already exists.'):
                    order_line_item_ids.append(OrderLineItemID)

        for order_line_item_id in order_line_item_ids:
            logging.info("Looking for order: %s", order_line_item_id)
            order_line_ids = rpc.read('sale.order.line',
                                      [[['web_orderline_id', '=', order_line_item_id],
                                        ['order_id.state', '!=', 'cancel']
                                        ]]
                                      )
            for order_line_id in order_line_ids:
                picking_ids = rpc.search('stock.picking', [[['sale_id', '=', order_line_id['order_id'][0]], ['tracking_number', '!=', False]]])
                for picking_id in picking_ids:
                    logging.info("Store notified for: %s", picking_id)
                    rpc.write('stock.picking', picking_id, {'store_notified': True})


def main():
    slack.notify_slack(this_module_name, "Started cron: %s" % this_module_name)
    now = datetime.utcnow()
    dir_location = '/var/tmp/tracking/%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)
    for store in credentials:
        if credentials[store]['type'] == 'ebay':
            # if store != 'visionary':
            #     continue
            ebay_send_tracking_info(store, dir_location)
        else:
            continue
    slack.notify_slack(this_module_name, "Ended cron: %s" % this_module_name)


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")


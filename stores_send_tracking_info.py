# -*- coding: utf-8 -*-

from datetime import datetime
from lxml import etree
import json
import lmslib
import logging
import simplejson
import os
import sys
import time
import uuid
import xmlrpclib
import zipfile
# import slack
import opsystConnection

thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

this_module_name = os.path.basename(sys.modules['__main__'].__file__)
dir_location = '/var/tmp/tracking'
odoo_url = config.get('odoo_url')
odoo_db = config.get('odoo_db')
odoo_uid = config.get('odoo_uid')
odoo_password = config.get('odoo_password')
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(odoo_url))
logging.basicConfig(level=logging.INFO)
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
odoo_password = config.get('odoo_password')
stores = config.get('stores').split(',')
Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db,
                                               odoo_db_host=odoo_db_host,
                                               odoo_db_user=odoo_db_user,
                                               odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials(stores)
# slack_bot_token = "xoxb-312554961652-uSmliU84rFhnUSBq9YdKh6lS"
# slack_channel_id = "CAM3ND487"
# slack_cron_info_channel_id = "CALDV44N9"


def ebay_send_tracking_info(store, dir_location):
    logging.info('\n\nProcessing tracking upload for %s...' % store)
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

    picking_ids = models.execute_kw(odoo_db, odoo_uid, odoo_password, 'stock.picking', 'search_read',
                                    [[['tracking_number', '!=', False],
                                      ['store_id.code', '=', store],
                                      ['store_notified', '=', False],
                                      ['write_date', '>=', '2018-01-01 00:00:00']
                                      ]],
                                    {'fields': ['tracking_number', 'sale_id', 'carrier_id']}
                                    )
    # logging.info("Pickings: %s", picking_ids)
    # slack.notify_slack(this_module_name, "Pickings amount: %s" % len(picking_ids))
    has_upload = False
    order_line_list = []

    for picking in picking_ids:
        if not picking['sale_id']:
            continue
        order_lines = models.execute_kw(odoo_db, odoo_uid, odoo_password, 'sale.order.line', 'search_read',
                                        [[['order_id', '=', picking['sale_id'][0]]]],
                                        {'fields': ['web_orderline_id']}
                                        )
        order_id = models.execute_kw(odoo_db, odoo_uid, odoo_password, 'sale.order', 'search_read',
                                     [[['id', '=', picking['sale_id'][0]]]],
                                     {'fields': ['web_order_id']}
                                     )
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
        environment = lmslib.PRODUCTION
        logging.info('Creating upload job')
        build_uuid = uuid.uuid4()
        create_job = lmslib.CreateUploadJob(environment, credentials[store])
        create_job.buildRequest('SetShipmentTrackingInfo', 'gzip', build_uuid)
        resp = create_job.sendRequest()
        logging.info('create_job SendRequest: %s', resp)
        response, resp_struct = create_job.getResponse()
        if response == 'Success':
            jobId = resp_struct.get('jobId', None)
            fileReferenceId = resp_struct.get('fileReferenceId', None)
            logging.info('Job created: job id %s, file reference id %s' % (jobId, fileReferenceId))
        elif response == 'Failure':
            logging.error('%s' % resp_struct.get('message', None))
            logging.info("Trying to abort stuck jobs ...")
            for job_type in ['Created', 'InProcess', 'Failed', 'Scheduled']:
                get_jobs = lmslib.GetJobs(environment, credentials[store])
                get_jobs.buildRequest(jobType='SetShipmentTrackingInfo', jobStatus=job_type)
                resp = get_jobs.sendRequest()
                logging.info('get_jobs SendRequest: %s', resp)
                response, resp_struct = get_jobs.getResponse()
                for r in resp_struct:
                    abort_job = lmslib.AbortJob(environment, credentials[store])
                    abort_job.buildRequest(r['jobId'])
                    abort_resp = abort_job.sendRequest()
                    logging.info('abort_job SendRequest: %s', abort_resp)
                    response, resp_struct = abort_job.getResponse()
                    logging.info("Abortion: %s %s", response, resp_struct)
            return
        time.sleep(10)
        logging.info('Uploading file')
        upload_file = lmslib.UploadFile(environment, credentials[store])
        upload_file.buildRequest(jobId, fileReferenceId, filename)
        resp = upload_file.sendRequest()
        logging.info('SendRequest: %s', resp)
        response, response_dict = upload_file.getResponse()
        if response == 'Success':
            logging.info('File uploaded')
        else:
            logging.error('%s' % resp_struct.get('message', None))
            return

        time.sleep(10)

        logging.info('Starting upload job')
        start_upload_job = lmslib.StartUploadJob(environment, credentials[store])
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
        job_status = lmslib.GetJobStatus(environment, credentials[store])
        job_status.buildRequest(jobId)
        while True:
            response = job_status.sendRequest()
            logging.info('SendRequest: %s', response)
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
        download_file = lmslib.DownloadFile(environment, credentials[store])
        download_file.buildRequest(jobId, downloadFileId)
        resp = download_file.sendRequest()
        logging.info('download_file SendRequest: %s', resp)
        path = '/var/tmp/tracking/%s_response.zip' % datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
        response, resp_struc = download_file.getResponse(path)
        if response == 'Success':
            logging.info('Successfully downloaded response.')
        elif response == "Failure":
            logging.error('%s' % resp_struct.get('message', None))
            # slack.notify_slack(this_module_name, "Error: %s" % resp_struct.get('message', None), slack_bot_token=slack_bot_token,  slack_cron_info_channel_id=slack_channel_id)
            return

        logging.info('Unzipping the download.')
        zip_ref = zipfile.ZipFile(path, 'r')
        zip_ref.extractall(dir_location)
        zip_ref.close()
        os.remove(path)

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
            order_line_ids = models.execute_kw(odoo_db, odoo_uid, odoo_password, 'sale.order.line', 'search_read',
                                               [[['web_orderline_id', '=', order_line_item_id],
                                                 ['order_id.state', '!=', 'cancel']
                                                 ]],
                                               {'fields': ['order_id']}
                                               )
            for order_line_id in order_line_ids:
                picking_ids = models.execute_kw(odoo_db, odoo_uid, odoo_password, 'stock.picking', 'search',
                                                [[['sale_id', '=', order_line_id['order_id'][0]],
                                                  ['tracking_number', '!=', False]
                                                  ]])
                for picking_id in picking_ids:
                    logging.info("Store notified for: %s", picking_id)
                    models.execute_kw(odoo_db, odoo_uid, odoo_password, 'stock.picking', 'write', [[picking_id], {
                        'store_notified': True
                    }])


def main():
    # slack.notify_slack(this_module_name, "Started cron: %s" % this_module_name)
    # stores = config['stores']
    now = datetime.utcnow()
    dir_location = '/var/tmp/tracking/%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)
    for store in credentials:
        if credentials[store]['type'] == 'ebay':
            ebay_send_tracking_info(store, dir_location)
        else:
            continue
    # slack.notify_slack(this_module_name, "Ended cron: %s" % this_module_name)


if __name__ == "__main__":
    main()

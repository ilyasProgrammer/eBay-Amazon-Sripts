"""
Reprice MAPM listings that was out of stock and become in stock.
"""

import sys
import csv
import ftplib
import logging
import os
import simplejson
import time
import zipfile

from datetime import datetime

import opsystConnection
import amzConnection

thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
stores = config.get('stores').split(',')
Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials(stores)

this_module_name = os.path.basename(sys.modules['__main__'].__file__)
LOG_FILE = '/mnt/vol/log/cron/reprice_wh_mapm_listings_%s.log' % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)


def get_underpriced_listings(store):
    return Connection.odoo_execute("""
        SELECT PL.name, PTL.total_min_cost FROM product_template_listed PTL
        LEFT JOIN product_listing PL ON PL.product_tmpl_id = PTL.product_tmpl_id
        WHERE PL.state = 'active' AND PTL.total_min_cost > 0 AND PL.current_min_price > 0
        AND PL.name LIKE 'MAPM%%' AND PL.store_id =  %s AND PL.listing_type <> 'fba'
        AND PTL.wh_qty > 0
        AND (PL.do_not_reprice IS NULL OR PL.do_not_reprice = False)
    """, [credentials[store]['id']])


def amazon_upload_pricing_to_store(store, now):
    logging.info("Uploading price file for Amazon")
    rows = get_underpriced_listings(store)
    if rows:
        xml_body = ''
        message_counter = 1
        for row in rows:
            price = row['total_min_cost'] * 1.23
            if row['name'].startswith('MAPM-PBL'):
                price = row['total_min_cost'] * 1.5
            elif row['name'].startswith('MAPM'):
                price = row['total_min_cost'] * 1.45
            xml_body += "<Message>"
            xml_body += "<MessageID>%s</MessageID>" % message_counter
            xml_body += "<Price>"
            xml_body += "<SKU>%s</SKU>" % row['name']
            xml_body += "<StandardPrice currency='USD'>%s</StandardPrice>" % round(price, 2)
            xml_body += "</Price>"
            xml_body += "</Message>"
            message_counter += 1
        merchant_dentifier = '_POST_PRODUCT_PRICING_DATA_' + now.strftime('%Y-%m-%d-%H-%M-%S')
        xml = "<?xml version='1.0' encoding='utf-8'?>"
        xml += "<AmazonEnvelope xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='amzn-envelope.xsd'>"
        xml += "<Header>"
        xml += "<DocumentVersion>1.0</DocumentVersion>"
        xml += "<MerchantIdentifier>%s</MerchantIdentifier>" % merchant_dentifier
        xml += "</Header>"
        xml += "<MessageType>Price</MessageType>"
        xml += xml_body
        xml += "</AmazonEnvelope>"
        md5value = amzConnection.get_md5(xml)
        params = {
            'ContentMD5Value': md5value,
            'Action': 'SubmitFeed',
            'FeedType': '_POST_PRODUCT_PRICING_DATA_',
            'PurgeAndReplace': 'false'
        }
        amz_request = amzConnection.AmzConnection(
            access_id=credentials[store]['access_id'],
            marketplace_id=credentials[store]['marketplace_id'],
            seller_id=credentials[store]['seller_id'],
            secret_key=credentials[store]['secret_key']
        )
        response = amz_request.process_amz_request('POST', '/Feeds/2009-01-01', params, xml)
        feed_id = None
        if 'FeedSubmissionInfo' in response['SubmitFeedResponse']['SubmitFeedResult']:
            feed_info = response['SubmitFeedResponse']['SubmitFeedResult']['FeedSubmissionInfo']
            feed_id = feed_info['FeedSubmissionId']['value']
            logging.info('Feed submitted %s' % feed_id)
        params = {
            'Action': 'GetFeedSubmissionResult',
            'FeedSubmissionId': feed_id
        }
        time.sleep(15)
        while True:
            try:
                response = amz_request.process_amz_request('POST', '/Feeds/2009-01-01', params)
                status = response['AmazonEnvelope']['Message']['ProcessingReport']['StatusCode']['value']
                if status == 'Complete':
                    logging.info('Feed processed!!!')
                    break
                time.sleep(60)
            except Exception as e:
                logging.error(e)
                time.sleep(60)


def main():
    now = datetime.now()
    for store in stores:
        if credentials[store]['type'] == 'amz':
            amazon_upload_pricing_to_store(store, now)


if __name__ == "__main__":
    main()

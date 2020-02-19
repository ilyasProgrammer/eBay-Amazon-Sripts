"""
Update total min cost of listed products
"""

import argparse
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
import sys
import getpass
import sys
import slack

current_user = getpass.getuser()
logging.getLogger('requests').setLevel(logging.ERROR)
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

odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')
stores = ['sinister']

LOG_FILE = '/mnt/vol/log/cron/amz_shipping_template_%s.log' % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)

Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials(stores)

DEFAULT_SHIPPING_TEMPLATE = '5-Days Handling Time Amazon Template'
PRIME_SHIPPING_TEMPLATE = 'Prime template'
MIN_QTY_FOR_PRIME = 10  # Min qty for a warehouse item to be prime eligible


def mark_listings_as_prime_illigible(store):
    logging.info("Marking listings NON PRIME ...")
    query = """
        UPDATE product_listing
        SET
            listing_type = 'normal'
        FROM (
            SELECT PL.id as listing_id FROM product_template_listed PTL
            LEFT JOIN product_listing PL ON PL.product_tmpl_id = PTL.product_tmpl_id
            LEFT JOIN product_template pt on ptl.product_tmpl_id = pt.id
            WHERE PL.state = 'active' and PTL.state = 'active' AND (PL.listing_type IS NULL OR PL.listing_type = 'fbm')
            AND PTL.wh_qty > %s AND PL.store_id = %s AND ( pt.length + 2*pt.width + 2*pt.height) > 135
        ) AS RES WHERE RES.listing_id = product_listing.id
    """
    # query = """
    #         UPDATE product_listing
    #         SET
    #             listing_type = 'normal'
    #         FROM (
    #             SELECT PL.id as listing_id FROM product_template_listed PTL
    #             LEFT JOIN product_listing PL ON PL.product_tmpl_id = PTL.product_tmpl_id
    #             LEFT JOIN product_template pt on ptl.product_tmpl_id = pt.id
    #             WHERE PL.state = 'active' and PTL.state = 'active' AND PL.store_id = %s
    #         ) AS RES WHERE RES.listing_id = product_listing.id
    #     """
    Connection.odoo_execute(query, [MIN_QTY_FOR_PRIME, credentials[store]['id']], commit=True)


def mark_listings_as_prime_elligible(store):
    logging.info("Marking listings as prime eligible...")
    query = """
        UPDATE product_listing
        SET
            listing_type = 'fbm'
        FROM (
            SELECT PL.id as listing_id FROM product_template_listed PTL
            LEFT JOIN product_listing PL ON PL.product_tmpl_id = PTL.product_tmpl_id
            LEFT JOIN product_template pt on ptl.product_tmpl_id = pt.id
            WHERE PL.state = 'active' and PTL.state = 'active' AND (PL.listing_type IS NULL OR PL.listing_type = 'normal')
            AND PTL.wh_qty > %s AND PL.store_id = %s AND ( pt.length + 2*pt.width + 2*pt.height) < 135
        ) AS RES WHERE RES.listing_id = product_listing.id
    """
    Connection.odoo_execute(query, [MIN_QTY_FOR_PRIME, credentials[store]['id']], commit=True)


def get_xml_content(store, now):
    query = """
        SELECT PL.title, PL.name, PTL.wh_qty FROM product_template_listed PTL
        LEFT JOIN product_listing PL ON PL.product_tmpl_id = PTL.product_tmpl_id
        WHERE PL.state = 'active' and PTL.state = 'active' AND PL.listing_type = 'fbm'
        AND PL.store_id = %s
    """
    rows = Connection.odoo_execute(query, [credentials[store]['id']])
    logging.info("Total Rows: %s", len(rows))
    if not rows:
        return False

    FulfillmentDate = now.strftime('%Y-%m-%d'+'T'+'%H:%M:%S'+'.000Z')
    MerchantIdentifier = '%s-' %now.strftime('Shipping-Template-Update-%Y-%m-%d-%H-%M-%S')
    counter = 1
    xml_body = ''
    for row in rows:
        if not row['title']:
            row['title'] = 'Make Auto Parts Manufacturing'

        shipping_template = DEFAULT_SHIPPING_TEMPLATE
        if row['wh_qty'] > MIN_QTY_FOR_PRIME:
            shipping_template = PRIME_SHIPPING_TEMPLATE

        xml_body += "<Message>"
        xml_body += "<MessageID>{MessageID}</MessageID>".format(MessageID=str(counter))
        xml_body += "<OperationType>PartialUpdate</OperationType>"
        xml_body += "<Product>"
        xml_body += "<SKU>{SKU}</SKU>".format(SKU=row['name'])
        xml_body += "<DescriptionData>"
        xml_body += "<Title>{Title}</Title>".format(Title=row['title'].replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&apos;'))
        xml_body += "<MerchantShippingGroupName>{Template}</MerchantShippingGroupName>".format(Template=shipping_template)
        xml_body += "</DescriptionData>"
        xml_body += "</Product>"
        xml_body += "</Message>\n"
        counter += 1

    xml = "<?xml version='1.0' encoding='utf-8'?>"
    xml += "<AmazonEnvelope xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='amzn-envelope.xsd'>"
    xml += "<Header>"
    xml += "<DocumentVersion>1.0</DocumentVersion>"
    xml += "<MerchantIdentifier>{MerchantIdentifier}</MerchantIdentifier>".format(MerchantIdentifier=MerchantIdentifier)
    xml += "</Header>"
    xml += "<MessageType>Product</MessageType>"
    xml += "<PurgeAndReplace>false</PurgeAndReplace>\n"
    xml += xml_body
    xml += "</AmazonEnvelope>"

    return xml


def get_xml_content_normal(store, now):
    query = """
        SELECT PL.title, PL.name, PTL.wh_qty FROM product_template_listed PTL
        LEFT JOIN product_listing PL ON PL.product_tmpl_id = PTL.product_tmpl_id
        WHERE PL.state = 'active' and PTL.state = 'active' AND PL.listing_type = 'normal'
        AND PL.store_id = %s
    """
    rows = Connection.odoo_execute(query, [credentials[store]['id']])
    if not rows:
        return False

    FulfillmentDate = now.strftime('%Y-%m-%d'+'T'+'%H:%M:%S'+'.000Z')
    MerchantIdentifier = '%s-' %now.strftime('Shipping-Template-Update-%Y-%m-%d-%H-%M-%S')
    counter = 1
    xml_body = ''
    for row in rows:
        if not row['title']:
            row['title'] = 'Make Auto Parts Manufacturing'

        shipping_template = DEFAULT_SHIPPING_TEMPLATE
        xml_body += "<Message>"
        xml_body += "<MessageID>{MessageID}</MessageID>".format(MessageID=str(counter))
        xml_body += "<OperationType>PartialUpdate</OperationType>"
        xml_body += "<Product>"
        xml_body += "<SKU>{SKU}</SKU>".format(SKU=row['name'])
        xml_body += "<DescriptionData>"
        xml_body += "<Title>{Title}</Title>".format(Title=row['title'].replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&apos;'))
        xml_body += "<MerchantShippingGroupName>{Template}</MerchantShippingGroupName>".format(Template=shipping_template)
        xml_body += "</DescriptionData>"
        xml_body += "</Product>"
        xml_body += "</Message>\n"
        counter += 1

    xml = "<?xml version='1.0' encoding='utf-8'?>"
    xml += "<AmazonEnvelope xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='amzn-envelope.xsd'>"
    xml += "<Header>"
    xml += "<DocumentVersion>1.0</DocumentVersion>"
    xml += "<MerchantIdentifier>{MerchantIdentifier}</MerchantIdentifier>".format(MerchantIdentifier=MerchantIdentifier)
    xml += "</Header>"
    xml += "<MessageType>Product</MessageType>"
    xml += "<PurgeAndReplace>false</PurgeAndReplace>\n"
    xml += xml_body
    xml += "</AmazonEnvelope>"

    return xml


def build_shipping_template_update_upload_file(store, now, ltype):
    dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)
    if ltype == 'fbm':
        xml_content = get_xml_content(store, now)
    elif ltype == 'normal':
        xml_content = get_xml_content_normal(store, now)

    if xml_content:
        feed_filename = dir_location + '/%s_shipping_upload_%s_%s_%s.xml' %(store, now.hour, now.minute, now.second)
        feed_file = open(feed_filename, 'wb')
        feed_file.write(xml_content)
        feed_file.close()
        return feed_filename
    logging.error("There are no listings to update.")
    return False


def amz_upload_listing_shipping_templates(store, now, ltype):
    logging.info("Updating shipping template of listings ... " + ltype)
    feed_filename = build_shipping_template_update_upload_file(store, now, ltype)
    logging.info("Uploading %s..." % feed_filename)
    if not feed_filename:
        return False
    
    amz_request = amzConnection.AmzConnection(
        access_id=credentials[store]['access_id'],
        marketplace_id=credentials[store]['marketplace_id'],
        seller_id=credentials[store]['seller_id'],
        secret_key=credentials[store]['secret_key']
    )

    logging.info('Submitting shipping template update file for %s' % store)
    feed_file = open(feed_filename , 'r')
    feed_content = feed_file.read()
    md5value = amzConnection.get_md5(feed_content)

    params = {
        'ContentMD5Value': md5value,
        'Action': 'SubmitFeed',
        'FeedType': '_POST_PRODUCT_DATA_',
        'PurgeAndReplace': 'false'
    }

    response = amz_request.process_amz_request('POST', '/Feeds/2009-01-01', params, feed_content)
    feed_id = None
    if 'FeedSubmissionInfo' in response['SubmitFeedResponse']['SubmitFeedResult']:
        feed_info = response['SubmitFeedResponse']['SubmitFeedResult']['FeedSubmissionInfo']
        feed_id = feed_info['FeedSubmissionId']['value']
        logging.info('Feed submitted %s' % feed_id)

    params = {'Action': 'GetFeedSubmissionResult', 'FeedSubmissionId': feed_id}
    time.sleep(15)
    while True:
        try:
            response = amz_request.process_amz_request('POST', '/Feeds/2009-01-01', params)
            status = response['AmazonEnvelope']['Message']['ProcessingReport']['StatusCode']['value']
            if status == 'Complete':
                print response
                logging.info('FEED PROCESSED!!!')
                break
            time.sleep(70)
        except Exception as e:
            logging.info('Not Ready')
            logging.info(e)
            logging.info(response)
            time.sleep(70)
    return True


def main():
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    now = datetime.now()
    for store in stores:
        if credentials[store]['type'] == 'amz':
            mark_listings_as_prime_illigible(store)
            mark_listings_as_prime_elligible(store)
            amz_upload_listing_shipping_templates(store, now, 'fbm')
            now = datetime.now()
            amz_upload_listing_shipping_templates(store, now, 'normal')
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

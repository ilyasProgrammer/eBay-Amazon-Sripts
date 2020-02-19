# -*- coding: utf-8 -*-

"""
In v2_amz_upload_stock_and_price, FBM listings sharing same asin as FBA listings
should not update stocks using the usual business logic.

We wish to deactivate FBM listings when FBA listings are active
and activate FBA listings when FBM listings are inactive.

Should run hourly (?)
"""

import argparse
import logging
import os
import simplejson
import time
from datetime import datetime
import opsystConnection
import amzConnection
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
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')
stores = ['sinister']

LOG_FILE = '/mnt/vol/log/cron/amz_switch_fbm_and_fba_%s.log' %datetime.today().strftime('%Y_%m')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', filename=LOG_FILE, datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger().addHandler(slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id))

Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials(stores)
DT_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_xml_content(store, qty_dict):
    now = datetime.now()
    xml_content = ""
    if qty_dict:
        merchant_identifier = 'InvFeed_%s_%s_%s_%s_%s_%s' % (now.year, now.month, now.day, now.hour, now.minute, now.second)
        xml_content += "<?xml version='1.0' encoding='utf-8'?>\n"
        xml_content += "<AmazonEnvelope xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='amzn-envelope.xsd'>\n"
        xml_content += "<Header>\n"
        xml_content += "<DocumentVersion>1.0</DocumentVersion>\n"
        xml_content += "<MerchantIdentifier>%s</MerchantIdentifier>\n" % merchant_identifier
        xml_content += "</Header>\n"
        xml_content += "<MessageType>OrderFulfillment</MessageType>\n"
        xml_content += "<PurgeAndReplace>false</PurgeAndReplace>\n"

        counter = 1
        for asin in qty_dict:
            xml_content += "<Message>"
            xml_content += "<MessageID>%s</MessageID>" % counter
            xml_content += "<OperationType>PartialUpdate</OperationType>"
            xml_content += "<Inventory>"
            xml_content += "<SKU>%s</SKU>" % qty_dict[asin]['sku']

            if qty_dict[asin]['fba_active']:
                qty = 0
            else:
                qty = min(qty_dict[asin]['wh_qty'] + qty_dict[asin]['vendor_qty'], 80)
            xml_content += "<Quantity>%s</Quantity>" % qty
            if not (qty_dict[asin]['wh_qty'] > 0):
                xml_content += "<FulfillmentLatency>3</FulfillmentLatency>"
            xml_content += "</Inventory>"
            xml_content += "</Message>\n"

            counter += 1

        xml_content += "</AmazonEnvelope>"
    return xml_content


def build_inventory_upload_file(store, qty_dict):
    now = datetime.now()
    logging.info('Creating inventory file for %s' % store)
    dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)
    xml_content = get_xml_content(store, qty_dict)

    if xml_content:
        feed_filename = dir_location + '/%s_fbm_fba_switch_upload_%s_%s_%s.xml' % (store, now.hour, now.minute, now.second)
        feed_file = open(feed_filename, 'wb')
        feed_file.write(xml_content)
        feed_file.close()
        return feed_filename
    logging.error("No inventory updates to upload to %s" % store)
    return False


def upload_quantities_to_store(store, qty_dict):
    feed_filename = build_inventory_upload_file(store, qty_dict)
    if not feed_filename:
        return False

    amz_request = amzConnection.AmzConnection(
        access_id=credentials[store]['access_id'],
        marketplace_id=credentials[store]['marketplace_id'],
        seller_id=credentials[store]['seller_id'],
        secret_key=credentials[store]['secret_key']
    )

    logging.info('Submitting inventory file for %s' % store)
    feed_file = open(feed_filename, 'r')
    feed_content = feed_file.read()
    md5value = amzConnection.get_md5(feed_content)

    params = {
        'ContentMD5Value': md5value,
        'Action': 'SubmitFeed',
        'FeedType': '_POST_INVENTORY_AVAILABILITY_DATA_',
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
                logging.info('FEED PROCESSED!!!')
                break
            time.sleep(60)
        except:
            time.sleep(60)
    return True


def get_quantities_to_upload(fbm_dict, fba_dict):
    qty_dict = fbm_dict
    for fbm_asin in fbm_dict:
        if fbm_asin in fba_dict and fba_dict[fbm_asin]['qty'] > 0 and fba_dict[fbm_asin]['availability'] != 'Unknown':
            qty_dict[fbm_asin]['fba_active'] = True
        else:
            qty_dict[fbm_asin]['fba_active'] = False
    return qty_dict


def get_fba_availability(store, fba_skus):
    fba_dict = {}
    amz_request = amzConnection.AmzConnection(
        access_id=credentials[store]['access_id'],
        marketplace_id=credentials[store]['marketplace_id'],
        seller_id=credentials[store]['seller_id'],
        secret_key=credentials[store]['secret_key']
    )

    fba_skus_len = len(fba_skus)

    # Max of 50 SKUs per request.
    # See http://docs.developer.amazonservices.com/en_US/fba_inventory/FBAInventory_ListInventorySupply.html

    batches = fba_skus_len / 50
    if fba_skus_len % 50 == 0:
        batches -= 1

    batch = 0
    while batch <= batches:
        logging.info('Fetching FBA availability of batch %s of %s...' % (batch, batches))
        batch_rows = fba_skus[batch * 50: min(fba_skus_len, (batch + 1) * 50)]
        params = {
            'Action': 'ListInventorySupply',
            'MarketplaceId': credentials[store]['marketplace_id'],
        }
        counter = 1
        for sku in batch_rows:
            params['SellerSkus.member.' + str(counter)] = sku
            counter += 1

        response = amz_request.process_amz_request('POST', '/FulfillmentInventory/2010-10-01', params)

        qtys = response['ListInventorySupplyResponse']['ListInventorySupplyResult']['InventorySupplyList']['member']
        for q in qtys:
            if 'ASIN' in q:
                fba_dict[q['ASIN']['value']] = {
                    'qty': int(q['TotalSupplyQuantity']['value'])
                }
                if 'EarliestAvailability' in q:
                    fba_dict[q['ASIN']['value']]['availability'] = q['EarliestAvailability']['TimepointType']['value']
        batch += 1

    return fba_dict


def update_fba_qty_in_opsyst(fba_dict):
    qty_cases = ''
    asins = []
    for asin in fba_dict:
        qty_cases += "WHEN asin='%s' THEN %s " % (asin, fba_dict[asin]['qty'])
        asins.append(asin)
    query = "UPDATE product_listing SET fba_qty = (CASE %s END) " % qty_cases
    Connection.odoo_execute(query + """
        WHERE asin IN %s AND listing_type = 'fba' AND state = 'active'
    """, [asins], commit=True)
    Connection.odoo_execute("""
        UPDATE product_listing SET fba_qty = 0
        WHERE asin NOT IN %s AND listing_type = 'fba' AND state = 'active'
    """, [asins], commit=True)


def get_fba_skus(store):
    fba_skus = []
    fba_skus_rows = Connection.odoo_execute("""
        SELECT PL.name FROM product_listing PL
        WHERE PL.listing_type = 'fba' AND PL.state = 'active' AND PL.store_id = %s
    """, [credentials[store]['id']])
    for row in fba_skus_rows:
        fba_skus.append(row['name'])
    return fba_skus


def get_fbm_availability(store):
    fbm_dict = {}
    fbm_rows = Connection.odoo_execute("""
        SELECT 
            PL.name,
            PL.asin, 
            (CASE WHEN PTL.wh_qty > 0 THEN PTL.wh_qty ELSE 0 END) as wh_qty,
            (CASE WHEN PTL.vendor_qty > 0 THEN PTL.vendor_qty ELSE 0 END) as vendor_qty
        FROM product_listing PL
        LEFT JOIN product_template_listed PTL ON PL.product_tmpl_id = PTL.product_tmpl_id
        WHERE PL.listing_type <> 'fba' AND PL.asin IN (
            SELECT PL.asin FROM product_listing PL WHERE PL.listing_type = 'fba'
            AND PL.state = 'active' AND PL.store_id = %s
        ) AND PL.state = 'active' AND PL.store_id = %s
    """, [credentials[store]['id'], credentials[store]['id']])
    for row in fbm_rows:
        fbm_dict[row['asin']] = {
            'wh_qty': row['wh_qty'],
            'vendor_qty': row['vendor_qty'],
            'sku': row['name']
        }
    return fbm_dict


def main():
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    for store in stores:
        slack.notify_slack(this_module_name, "Store: %s" % str(store))
        fbm_dict = get_fbm_availability(store)
        fba_skus = get_fba_skus(store)
        fba_dict = get_fba_availability(store, fba_skus)
        qty_dict = get_quantities_to_upload(fbm_dict, fba_dict)
        update_fba_qty_in_opsyst(fba_dict)
        upload_quantities_to_store(store, qty_dict)
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    main()

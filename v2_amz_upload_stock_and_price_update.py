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
stores = config.get('stores').split(',')

LOG_FILE = '/mnt/vol/log/cron/amz_repricer_%s.log' % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)

Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user,
                                               odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials(stores)


def get_xml_content(store, now):
    xml_content = ""
    rows = Connection.odoo_execute("""
        SELECT PL.name, PTL.wh_qty, PTL.vendor_qty, 
        pt.length, pt.width, pt.height, ( pt.length + 2*pt.width + 2*pt.height) as girth, pl.listing_type 
        FROM product_template_listed PTL
        LEFT JOIN product_listing PL ON PL.product_tmpl_id = PTL.product_tmpl_id
        LEFT JOIN product_template pt on ptl.product_tmpl_id = pt.id
        WHERE PL.store_id = %s AND PL.state = 'active'
        AND (PL.do_not_restock = False OR PL.do_not_restock IS NULL) AND PL.listing_type <> 'fba' AND PL.name NOT LIKE 'WHCO-%%'
        AND PL.asin NOT IN (
            SELECT PL.asin FROM product_listing PL
            WHERE PL.listing_type = 'fba' AND PL.state = 'active'
        )
    """, [credentials[store]['id']])

    if rows:
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
        for row in rows:
            qty = 0
            if row['wh_qty'] > 0 and row['vendor_qty'] > 0:
                qty = row['wh_qty'] + row['vendor_qty']
            elif row['wh_qty'] > 0:
                qty = row['wh_qty']
            elif row['vendor_qty'] > 0:
                qty = row['vendor_qty']

            if qty > credentials[store]['max_qty']:
                qty = credentials[store]['max_qty']

            xml_content += "<Message>"
            xml_content += "<MessageID>%s</MessageID>" % counter
            xml_content += "<OperationType>PartialUpdate</OperationType>"
            xml_content += "<Inventory>"
            xml_content += "<SKU>%s</SKU>" % row['name']
            # if row['wh_qty'] > 0 and row['listing_type'] == 'fbm' and float(row['girth']) > 75:
            if 0:
                xml_content += "<Quantity>0</Quantity>"
                logging.warning(row)
            else:
                xml_content += "<Quantity>%s</Quantity>" % qty
            if not (row['wh_qty'] > 0):
                xml_content += "<FulfillmentLatency>3</FulfillmentLatency>"
            xml_content += "</Inventory>"
            xml_content += "</Message>\n"

            counter += 1

        xml_content += "</AmazonEnvelope>"
    return xml_content


def build_inventory_upload_file(store, now):
    logging.info('Creating inventory file for %s' % store)
    dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)
    xml_content = get_xml_content(store, now)

    if xml_content:
        feed_filename = dir_location + '/%s_upload_%s_%s_%s.xml' % (store, now.hour, now.minute, now.second)
        feed_file = open(feed_filename, 'wb')
        feed_file.write(xml_content)
        feed_file.close()
        return feed_filename
    logging.error("No inventory updates to upload to %s" % store)
    return False


def amazon_upload_inventory_to_store(store, now):
    feed_filename = build_inventory_upload_file(store, now)
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
                logging.info('FEED %s PROCESSED!!!', feed_id)
                break
            time.sleep(60)
        except Exception as e:
            logging.info(e)
            time.sleep(60)
    return True


def get_min_and_max_prices_to_upload(store):
    rows = Connection.odoo_execute("""
        SELECT PL.name, PTL.wh_qty, PL.current_min_price, PL.current_max_price, PL.keep_manual, PL. manual_price, 
        REP.name as repricer, PTL.total_min_cost FROM product_template_listed PTL
        LEFT JOIN product_listing PL ON PL.product_tmpl_id = PTL.product_tmpl_id
        LEFT JOIN repricer_scheme REP on REP.id = PL.repricer_scheme_id
        WHERE PL.store_id = %s AND PL.state = 'active'
        AND (PL.do_not_reprice = False OR PL.do_not_reprice IS NULL) AND PL.name NOT LIKE 'WHCO-%%' AND (PTL.wh_qty > 0 OR PTL.vendor_qty > 0)
        AND PTL.total_min_cost > 0
    """, [credentials[store]['id']])

    rows_to_upload = []
    if rows:
        for row in rows:
            repricer = row['repricer']
            # min_price = 1.137 * row['total_min_cost']  # Mainly intended for dropship
            min_price = 1.137 * row['total_min_cost']  # Mainly intended for dropship

            # If cost is take from the warehouse, least margin should be 10%
            # if row['wh_qty'] > 0:  # green flag from Justin 2018-10-18
            #     min_price = 1.237 * row['total_min_cost']
            #     min_price = 1.237 * row['total_min_cost']

            # Premium brand listings have different multiplier
            if row['name'].startswith('MAPM'):
                repricer = 'MAPM listings'
                if row['name'].startswith('MAPM-PBL'):
                    min_price = 1.5 * row['total_min_cost']
                    # min_price = 1.5 * row['total_min_cost']
                else:
                    min_price = 1.45 * row['total_min_cost']
                    # min_price = 1.45 * row['total_min_cost']

            max_price = 2 * min_price

            if row['name'].startswith('MAPM') or repricer == 'MAPM listings':
                max_price = 0.05 + min_price

            if row['keep_manual'] and row['manual_price'] > 0:
                if row['total_min_cost'] < row['manual_price']:
                    min_price = row['manual_price']
                    max_price = row['manual_price']
                else:
                    logging.warning("Listing has keep_manual=TRUE incorrect! %s", row)

            if not row['current_min_price'] or "{0:.2f}".format(float(row['current_min_price'])) != "{0:.2f}".format(
                    float(min_price)) or row['current_max_price'] < row['current_min_price']:
                rows_to_upload.append((row['name'], "{0:.2f}".format(min_price * 1.07), "{0:.2f}".format(max_price * 1.07), repricer))

        return rows_to_upload


def amazon_upload_pricing_to_xsellco(store, now):
    logging.info("Uploading repricing file for Xsellco")

    rows_to_upload = get_min_and_max_prices_to_upload(store)
    if not rows_to_upload:
        logging.info("No repricer records need to be uploaded.")
        return False

    dir_location = '/mnt/vol/misc/inv_updates/%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)

    rows_len = len(rows_to_upload)
    batches = rows_len / 50000
    if rows_len % 50000 == 0:
        batches -= 1

    batch = 0
    while batch <= batches:
        batch_rows = rows_to_upload[batch * 50000: min(rows_len, (batch + 1) * 50000)]
        file_name = 'sinister_repricer_upload_%s_%s_%s_%s.csv' % (now.hour, now.minute, now.second, batch)
        file_path = dir_location + '/' + file_name
        with open(file_path, 'wb') as upload_file:
            writer = csv.writer(upload_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            repricer_columns = [
                'sku', 'marketplace', 'merchant_id', 'price_min', 'price_max',
                'unit_cost', 'unit_currency', 'shipping_cost', 'shipping_currency', 'pickpack_cost',
                'pickpack_currency', 'rrp_price', 'rrp_currency', 'vat_rate', 'fee_listing',
                'fba_fee', 'fba_currency', 'repricer_name', 'estimated_seller_count', 'lowest_landed_price',
                'my_price', 'my_shipping', 'fba', 'asin', 'open_date'
            ]
            writer.writerow(repricer_columns)
            for row in batch_rows:
                writer.writerow([
                    row[0], 'AUS', 'A31CNXFNX32WD5', row[1], row[2],
                    0, 'USD', 0, 'USD', 0,
                    'USD', 0, 'USD', 0, 0,
                    0, 'USD', row[3] or 'Off', '', '',
                    '', '', 'No', '', ''
                ])

        to_upload = dir_location + '/sinister_repricer_upload_%s_%s_%s_%s.zip' % (
        now.hour, now.minute, now.second, batch)
        with zipfile.ZipFile(to_upload, 'w') as myzip:
            myzip.write(file_path, arcname=file_name)
        ftp = ftplib.FTP("192.169.152.87")
        ftp.login("Repricer", "Th3!TGuy5#$!")
        file_name = "repricer_%s.zip" % datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        ftp.storbinary("STOR " + file_name, open(to_upload, "rb"), 1024)
        ftp.quit()
        slack.notify_slack('AMZ Repricer', "Sent file to FTP: %s" % file_name)
        batch += 1


def get_underpriced_listings(store):
    return Connection.odoo_execute("""
        SELECT PL.name, PTL.total_min_cost FROM product_template_listed PTL
        LEFT JOIN product_listing PL ON PL.product_tmpl_id = PTL.product_tmpl_id
        WHERE PL.state = 'active' AND PTL.total_min_cost > 0 AND PL.current_min_price > 0
        AND ROUND((1.137 * PTL.total_min_cost)::numeric, 2) > PL.current_price AND PL.store_id =  %s
        AND (PTL.wh_qty > 0 OR PTL.vendor_qty > 0)
        AND (PL.do_not_reprice IS NULL OR PL.do_not_reprice = False)
    """, [credentials[store]['id']])


def amazon_upload_pricing_to_store(store, now):
    logging.info("Uploading price file for Amazon")
    # Directly modify prices in amazon for listings that are underpriced due to highly likely delayed repricing from Xsellco

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
                logging.info(e)
                time.sleep(60)


def main():
    slack.notify_slack('AMZ Repricer', "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    now = datetime.now()
    for store in stores:
        if credentials[store]['type'] == 'amz':
            amazon_upload_inventory_to_store(store, now)
            amazon_upload_pricing_to_xsellco(store, now)
            amazon_upload_pricing_to_store(store, now)
    slack.notify_slack('AMZ Repricer', "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

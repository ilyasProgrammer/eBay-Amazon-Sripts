# -*- coding: utf-8 -*-

import argparse
import time
import re
import urllib
from requests import request
import rpc
import logging
import os
import base64
import simplejson
import time
import uuid
import zipfile
import utils
import bs4
import math
import hashlib
import hmac
from datetime import datetime, timedelta
from ebaysdk.trading import Connection as Trading
import lmslib
import opsystConnection
import amzConnection
from requests.exceptions import HTTPError
from xml.etree.ElementTree import ParseError as XMLError
us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
}

parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-test-only', action="store", dest="test_only")
args = vars(parser.parse_args())
test_only = bool(args['test_only'])
if args['test_only'] in ('0', 'False'):
    test_only = False
thisdir = os.path.abspath(os.path.dirname(__file__))
if test_only:
    fp = open(os.path.join(thisdir, 'test.json'), mode='r')
else:
    fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
logs_path = config.get('logs_path')
src_path = config.get('src_path')
# stores = config.get('stores').split(',')
# stores = ["ride", "rhino", "visionary", "sinister"]
store = "sinister"

LOG_FILE = os.path.join(logs_path + 'get_taxes.py___%s.log') % datetime.today().strftime('%Y_%m')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', filename=LOG_FILE, datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger().addHandler(logging.StreamHandler())
Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials([store])
environment = lmslib.PRODUCTION

taxes = rpc.read('account.tax', [])


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


class DataWrapper(object):
    """
        Text wrapper in charge of validating the hash sent by Amazon.
    """
    def __init__(self, data, header):

        def get_md5(content):
            base64md5 = base64.b64encode(hashlib.md5(content).digest())
            if base64md5[-1] == '\n':
                base64md5 = self.base64md5[0:-1]
            return base64md5

        self.original = data
        if 'content-md5' in header:
            hash_ = get_md5(self.original)
            if header['content-md5'] != hash_:
                raise MWSError("Wrong Contentlength, maybe amazon error...")

    @property
    def parsed(self):
        return self.original


class MWSError(Exception):
    """
        Main MWS Exception class
    """
    # Allows quick access to the response object.
    # Do not rely on this attribute, always check if its not None.
    response = None


def start_download_job(store, now):
    logging.info('Starting order report download job for %s...' % store)
    download_uuid = uuid.uuid4()
    download_job = lmslib.startDownloadJob(environment, credentials[store])
    download_job.buildRequest(jobType='OrderReport', uuid=download_uuid, createdFromDaysAgo=10, now=now)
    download_job.sendRequest()
    response, resp_struct = download_job.getResponse()
    if response == 'Success':
        job_id = resp_struct.get('jobId', None)
        logging.info("Started new download job for %s with job id %s..." % (store, job_id))
        return job_id
    logging.error("Failed to start new download job for %s..." % store)
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
                logging.info("JOB COMPLETED for %s! Yey!" % store)
                return resp_struct[0].get('fileReferenceId', None)
            elif status in ('Failed', 'Aborted'):
                logging.error("Job %s for %s" % (status, store))
                return False
            time.sleep(15)
        else:
            return False


def download_file(store, job_id, file_reference_id):
    logging.info("Downloading responses from %s" % store)
    dl_file = lmslib.DownloadFile(environment, credentials[store])
    dl_file.buildRequest(job_id, file_reference_id)
    response = dl_file.sendRequest()
    response, resp_struct = dl_file.getResponse()
    if response == 'Success':
        return True
    logging.info("Failed downloading file from %s." % store)
    return False


def unzip_and_parse_downloaded_file(store, job_id, now):
    dir_location = src_path + '/inv_updates/%s_%s_%s' % (now.year, now.month, now.day)
    if not os.path.exists(dir_location):
        os.makedirs(dir_location)

    logging.info("Unzipping the downloaded file...")
    zip_ref = zipfile.ZipFile('/tmp/data_responses.zip', 'r')
    zip_ref.extractall(dir_location)
    zip_ref.close()

    old_file_name = dir_location + '/%s_report.xml' % job_id
    new_file_name = dir_location + '/%s_order_report_%s_%s_%s.xml' % (store, now.hour, now.minute, now.second)
    os.rename(old_file_name, new_file_name)

    orders = []
    soup = bs4.BeautifulSoup(open(new_file_name, 'r').read(), 'lxml')
    for order in soup.find_all('order'):
        if float(order.salestaxpercent.text) > 0:
            orders.append({'order_id': order.orderid.text,
                           'tax_state': order.salestaxstate.text,
                           'tax_percent': float(order.salestaxpercent.text),
                           'tax_amount': float(order.salestaxamount.text)})
    return orders


def amz_get_orders():
    limit = 10
    offset = 0
    up_date_limit = '2017-06-28'
    while offset < 120000:
        odoo_orders = Connection.odoo_execute("""SELECT web_order_id, date_order 
                                                 FROM sale_order 
                                                 WHERE store_id = %s AND web_order_id is not NULL AND amount_tax = 0 AND date_order < '%s'
                                                 ORDER BY date_order DESC OFFSET %s LIMIT %s""" % (credentials[store]['id'], up_date_limit, offset, limit))
        offset += 10
        orders = []
        get_order_params = {}
        get_order_params['Action'] = 'ListOrderItems'
        for o_order in odoo_orders:
            get_order_params['AmazonOrderId'] = o_order['web_order_id']
            response = process_amz_request('GET', '/Orders/2013-09-01', get_order_params)
            if not response:
                continue
            try:
                logging.info('Getting amazon order: %s %s', o_order['web_order_id'], o_order['date_order'])
                result = response['ListOrderItemsResponse']['ListOrderItemsResult']['OrderItems']['OrderItem']
                if type(result) == list:
                    result = result[0]  # I hope same tax applied for all lines
                tax_percent = 100*float(result['ItemTax']['Amount']['value'])/float(result['ItemPrice']['Amount']['value'])
                tax_percent = truncate(tax_percent, 2)  # Because rounding might cause inaccuracy
                if tax_percent:
                    logging.info('Amazon order HAS tax: %s %s', o_order['web_order_id'], tax_percent)
                    orders.append({'order_id': get_order_params['AmazonOrderId'],
                                   'tax_state': None,
                                   'tax_percent': tax_percent,
                                   'tax_amount': float(result['ItemTax']['Amount']['value'])})
                else:
                    logging.info('NO tax: %s', o_order['web_order_id'])
            except Exception as e:
                logging.exception("Error %s", e)
            time.sleep(4)  # Sleep a bit. Otherwise amazon will ban us.
        write_taxes(orders)


def process_amz_request(verb, uri, extra_params=None, feed=None):
    now = datetime.utcnow()
    params = {
        'AWSAccessKeyId': credentials[store]['access_id'],
        'MarketplaceId.Id.1': credentials[store]['marketplace_id'],
        'SellerId': credentials[store]['seller_id'],
        'SignatureVersion': '2',
        'SignatureMethod': 'HmacSHA256',
        'Timestamp': now.strftime('%Y-%m-%d' + 'T' + '%H:%M:%S' + '.000Z')
    }
    if 'MarketplaceId' in extra_params:
        params.pop('MarketplaceId.Id.1', None)
    params.update(extra_params)

    request_description = '&'.join(['%s=%s' % (k, urllib.quote(params[k], safe='-_.~').encode('utf-8')) for k in sorted(params)])
    domain = 'https://mws.amazonservices.com'
    secret_key = credentials[store]['secret_key']

    signature = calc_signature(verb, uri, domain, secret_key, request_description)

    url = '%s%s?%s&Signature=%s' % (domain, uri, request_description, urllib.quote(signature))
    headers = {
        'User-Agent': 'python-amazon-mws/0.0.1 (Language=Python)'
    }
    res = None
    if params['Action'] == 'SubmitFeed':
        headers['Host'] = 'mws.amazonservices.com'
        headers['Content-Type'] = 'text/xml'
    elif params['Action'] == 'GetReport':
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
        parsed_response.response = response
        res = parsed_response._mydict
    except HTTPError, e:
        error = MWSError(str(e.response.text))
        error.response = e.response
        logging.exception("Error %s", str(error))
    return res


def calc_signature(verb, uri, domain, secret_key, request_description):
    sig_data = verb + '\n' + domain.replace('https://', '').lower() + '\n' + uri + '\n' + request_description
    return base64.b64encode(hmac.new(str(secret_key), sig_data, hashlib.sha256).digest())


def get_tax(tax=None, tax_percent=None):
    if tax_percent:
        for r in taxes:
            if r.get('state_id'):
                if type(r['state_id']) == list:
                    if abs(r['amount'] - tax_percent) <= 0.04:
                        return r
    if tax:
        for r in taxes:
            if r.get('state_id'):
                if type(r['state_id']) == list:
                    if us_state_abbrev[r['state_id'][1]] == tax:
                        return r
    return False


def write_taxes(orders):
    for order in orders:
        order_id = rpc.search('sale.order', [('web_order_id', '=', order['order_id'])])
        if order_id:
            order_line = rpc.search('sale.order.line', [('order_id', '=', order_id[0])])
            tax = get_tax(order['tax_state'], order['tax_percent'])
            if tax and us_state_abbrev.get(tax['state_id'][1]) and us_state_abbrev[tax['state_id'][1]] in ['WA', 'PA']:  # Amazon handling this taxes. We dont have to collect it
                continue
            elif tax:
                tax_id = tax['id']
                if tax_id:
                    if len(order_line) == 1:
                        rpc.write('sale.order.line', order_line[0], {'tax_id': [(6, 0, [tax_id])]})
                        logging.info('Tax added to line: %s %s %s', order_id, order_line, tax_id)
                    elif len(order_line) > 1:
                        for line_id in order_line:
                            rpc.write('sale.order.line', line_id, {'tax_id': [(6, 0, [tax_id])]})
                            logging.info('Line updated: %s %s %s', order_id, line_id, tax_id)
                else:
                    logging.warning('No tax found: %s %s %s', order_id, order['tax_state'], order['tax_percent'])
        else:
            logging.warning('Order not found: %s', order)


def remove_namespace(xml):
    regex = re.compile(' xmlns(:ns2)?="[^"]+"|(ns2:)|(xml:)')
    return regex.sub('', xml)


def truncate(f, n):
    return math.floor(f * 10 ** n) / 10 ** n


def main():
    amz_get_orders()


if __name__ == "__main__":
    main()

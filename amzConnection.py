# -*- coding: utf-8 -*-

from datetime import datetime
from requests import request
from requests.exceptions import HTTPError
from xml.etree.ElementTree import ParseError as XMLError

import base64
import hashlib
import hmac
import re
import time
import urllib
import utils
import logging


def remove_namespace(xml):
    regex = re.compile(' xmlns(:ns2)?="[^"]+"|(ns2:)|(xml:)')
    return regex.sub('', xml)


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


class MWSError(Exception):
    """
        Main MWS Exception class
    """
    # Allows quick access to the response object.
    # Do not rely on this attribute, always check if its not None.
    response = None


class AmzConnection(object):

    def __init__(self, *args, **kwargs):
        self.domain = 'https://mws.amazonservices.com'
        self.signature_version = '2'
        self.signature_method = 'HmacSHA256'
        self.access_id = ''
        self.marketplace_id = ''
        self.max_qty = 80
        self.seller_id = ''
        self.secret_key = ''
        self.code = ''

        for attr in kwargs.keys():
            setattr(self, attr, kwargs[attr])

    def calc_signature(self, verb, uri, request_description):
        sig_data = verb + '\n' + self.domain.replace('https://', '').lower() + '\n' + uri + '\n' + request_description
        return base64.b64encode(hmac.new(str(self.secret_key), sig_data, hashlib.sha256).digest())

    def process_amz_request(self, verb, uri, extra_params=None, feed=None):
        now = datetime.utcnow()
        params = {
            'AWSAccessKeyId': self.access_id,
            'MarketplaceId.Id.1': self.marketplace_id,
            'SellerId': self.seller_id,
            'SignatureVersion': self.signature_version,
            'SignatureMethod': self.signature_method,
            'Timestamp': now.strftime('%Y-%m-%d'+'T'+'%H:%M:%S'+'.000Z')
        }

        if 'MarketplaceId' in extra_params:
            params.pop('MarketplaceId.Id.1', None)

        params.update(extra_params)
        request_description = '&'.join(['%s=%s' % (k, urllib.quote(params[k], safe='-_.~').encode('utf-8')) for k in sorted(params)])

        signature = self.calc_signature(verb, uri, request_description)

        url = '%s%s?%s&Signature=%s' % (self.domain, uri, request_description, urllib.quote(signature))

        headers = {
            'User-Agent': 'python-amazon-mws/0.0.1 (Language=Python)'
        }
        headers['Content-Type'] = 'x-www-form-urlencoded'
        headers['Host'] = 'mws.amazonservices.com'
        if params['Action'] in ('SubmitFeed', 'GetReport'):
            if params['Action'] == 'SubmitFeed':
                headers['Content-Type'] = 'text/xml'
            else:
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

    def request_report(self, report_type):
        request_report_params = {
            'Action': 'RequestReport',
            'ReportType': report_type
        }
        response = self.process_amz_request('GET', '/Reports/2009-01-01', request_report_params)
        report_request_id = response['RequestReportResponse']['RequestReportResult']['ReportRequestInfo']['ReportRequestId']['value']
        logging.info('Requested report id %s for %s' % (report_request_id, self.code))
        return report_request_id

    def get_status_of_request(self, report_request_id):
        report_request_list_params = {
            'Action': 'GetReportRequestList',
            'ReportRequestIdList.Id.1': report_request_id
        }
        while True:
            response = self.process_amz_request('GET', '/Reports/2009-01-01', report_request_list_params)
            print response
            status = response['GetReportRequestListResponse']['GetReportRequestListResult']['ReportRequestInfo']['ReportProcessingStatus']['value']
            print status
            if status == '_DONE_':
                generated_report_id = response['GetReportRequestListResponse']['GetReportRequestListResult']['ReportRequestInfo']['GeneratedReportId']['value']
                logging.info('Generated report id %s for %s' % (generated_report_id, self.code))
                return generated_report_id
            elif status == '_CANCELLED_':
                return False
            time.sleep(20)

    def get_report(self, report_request_id, file_path):
        get_report_params = {
            'Action': 'GetReport',
            'ReportId': report_request_id
        }
        response = self.process_amz_request('POST', '/Reports/2009-01-01', get_report_params)
        inv_report_file = open(file_path, 'wb')
        inv_report_file.write(response)
        inv_report_file.close()

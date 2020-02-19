# -*- coding: utf-8 -*-

import argparse
import rpc
import logging
import os
import sys
import simplejson
import math
import getpass
import time
from datetime import datetime, timedelta
from ebaysdk.trading import Connection as Trading
import lmslib
import opsystConnection
import slack
import socket

us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'District of Columbia': 'DC',
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
logging.captureWarnings(True)
logging.getLogger('requests').setLevel(logging.ERROR)
current_user = getpass.getuser()
current_host = socket.gethostname()
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
logs_path = config.get('logs_path')
src_path = config.get('src_path')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')
# stores = config.get('stores').split(',')
stores = ["revive", "visionary", "ride", "rhino"]
# store = "visionary"

LOG_FILE = os.path.join(logs_path + 'get_taxes_ebay_%s.log') % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)

Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials(stores)
environment = lmslib.PRODUCTION

taxes = rpc.read('account.tax', [])
slack_critical_channel_id = 'CBA6AC8QL'


def ebay_get_orders(store):
    ebay_trading_api = Trading(domain='api.ebay.com', config_file=None, appid=credentials[store]['application_key'], devid=credentials[store]['developer_key'], certid=credentials[store]['certificate_key'],token=credentials[store]['auth_token'],siteid='100')
    period = datetime.strftime(datetime.now() - timedelta(days=60), '%Y-%m-%d')
    qry = """SELECT web_order_id FROM sale_order 
             WHERE store_id = %s and date_order >= '%s' 
             and state = 'sale' 
             AND web_order_id ~ '-' 
             AND amount_tax = 0""" % (credentials[store]['id'], period)
    odoo_orders = Connection.odoo_execute(qry)
    ords = [r['web_order_id'] for r in odoo_orders]
    chunks = split_on_chunks(ords, 20)
    cnt = 1
    for chunk in chunks:
        logging.info('Chunk %s', cnt)
        logging.info('Chunk data: %s', chunk)
        cnt += 1
        res = []
        orders = {'HasMoreOrders': 'true'}
        page = 1
        while orders['HasMoreOrders'] == 'true':
            try:
                orders = ebay_trading_api.execute('GetOrders', {'OrderIDArray': {'OrderID': chunk}, 'Pagination': {'EntriesPerPage': 10, 'PageNumber': page}}).dict()
                if orders['OrderArray']:
                    for order in orders['OrderArray']['Order']:
                        try:
                            # I do not iterate thru transactions cuz i believe that for multiline order only one tax type. So im going to apply tax from ShippingDetails for all lines of order.
                            if dv(order, ('ShippingDetails', 'SalesTax', 'SalesTaxState')):
                            # if order['ShippingDetails']['SalesTax']['SalesTaxState']:
                                amount = dv(order, ('ShippingDetails', 'SalesTax', 'SalesTaxAmount', 'value'))
                                if amount:
                                    logging.info('%s order HAS tax: %s %s %s', store, order['OrderID'], order['ShippingDetails']['SalesTax']['SalesTaxState'], amount)
                                    res.append({'order_id': order['OrderID'],
                                                'tax_state': order['ShippingDetails']['SalesTax']['SalesTaxState'],
                                                'tax_percent': float(order['ShippingDetails']['SalesTax']['SalesTaxPercent']),
                                                'tax_amount': amount})
                            else:
                                logging.info('%s order has NO tax: %s', store, order['OrderID'])
                        except Exception as e:
                            logging.error('order: %s', order)
                            logging.error('Error: %s', e.message)
            except Exception as e:
                logging.error('Error: %s', e.message)
                break
            page += 1
        write_taxes(res)


def get_tax(tax=None, tax_percent=None, order=None):
    try:
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
    except Exception as e:
        logging.error(e)
        logging.error(order)
    return False


def write_taxes(orders):
    for order in orders:
        order_id = rpc.search('sale.order', [('web_order_id', '=', order['order_id'])])
        try:
            if order_id:
                order_line = rpc.search('sale.order.line', [('order_id', '=', order_id[0]), ('tax_id', '=', False)])
                if len(order_line) == 0:
                    continue
                tax = get_tax(order['tax_state'], order['tax_percent'], order)
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
        except Exception as e:
            logging.error(e)
            slack.notify_slack(this_module_name, "Critical error: %s\nOrder: %s\nError: %s" % (this_module_name, order_id, e), slack_cron_info_channel_id=slack_critical_channel_id)


def truncate(f, n):
    return math.floor(f * 10 ** n) / 10 ** n


def main():
    slack.notify_slack(this_module_name, "Started cron: %s" % this_module_name)
    for store in stores:
        logging.info('Proceed %s', store)
        ebay_get_orders(store)
        logging.info('Ended %s', store)
        time.sleep(59)


def split_on_chunks(lst, num):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(lst), num):
        yield lst[i:i + num]


def dv(data, path, ret_type=None):
    # Deep value of nested dict. Return ret_type if cant find it
    for ind, el in enumerate(path):
        if data.get(el):
            return dv(data[el], path[ind+1:])
        else:
            return ret_type
    return data


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")


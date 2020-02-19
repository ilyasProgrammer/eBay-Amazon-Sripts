"""
Update repricer scheme of amazon listings
"""

from datetime import datetime, timedelta
import argparse
import simplejson
import os
import getpass
import sys
import slack
import socket
import logging
import opsystConnection

current_user = getpass.getuser()
current_host = socket.gethostname()
this_module_name = os.path.basename(sys.modules['__main__'].__file__)
parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-test-only', action="store", dest="test_only")
parser.add_argument('-update-all', action="store", dest="update_all")
parser.add_argument('-update-from', action="store", dest="update_from")
args = vars(parser.parse_args())
test_only = bool(args['test_only'])
update_all = bool(args['update_all'])
if test_only or current_host == 'pc':
    test_only = False
update_all = bool(args['update_all'])
if args['update_all'] in ('0', 'False'):
    update_all = False
update_from = int(args['update_from']) if args['update_from'] else 2

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

LOG_FILE = '/mnt/vol/log/cron/amz_repricing_scheme_%s.log' % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)
Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
DT_FORMAT = '%Y-%m-%d %H:%M:%S'


def main():
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    # Sample autoplus repricer record
    # {u'SKU': u'17Y-HWV-95L', u'ASIN': u'B01HH0JZ9W', u'RepricerName': u'2017-Feb', u'MinimumPrice': Decimal('325.75'), u'MaximumPrice': Decimal('651.50'), u'DateModified': datetime.datetime(2018, 1, 6, 12, 30, 30, 503000)}
    x_repricer_name_rows = Connection.autoplus_execute("""
        SELECT RepricerName FROM AmazonRepricer.dbo.Repricer2
        GROUP BY RepricerName
    """)
    x_repricer_names = [r['RepricerName'] for r in x_repricer_name_rows]

    o_repricer_name_rows = Connection.odoo_execute("""
        SELECT id, name FROM repricer_scheme
    """)
    repricer_mapping = {}
    o_repricer_names = []
    for r in o_repricer_name_rows:
        o_repricer_names.append(r['name'])
        repricer_mapping[r['name']] = r['id']

    new_repricers = [r for r in x_repricer_names if r not in o_repricer_names]
    if new_repricers:
        logging.error('No repricer in Opsyst: %s' % new_repricers)

    from_datetime = '2000-01-01 00:00:00'
    if not update_all:
        from_datetime = (datetime.utcnow() - timedelta(hours=update_from)).strftime(DT_FORMAT)

    x_repricer_rows = Connection.autoplus_execute("""
        SELECT SKU, MinimumPrice, MaximumPrice, RepricerName FROM AmazonRepricer.dbo.Repricer2
        WHERE DateModified >= %s
    """, [from_datetime])

    x_repricer_rows_len = len(x_repricer_rows)
    batches = x_repricer_rows_len / 1000
    if x_repricer_rows_len % 1000 == 0:
        batches -= 1

    logging.info("To update repricer data of %s listings in %s batches" %(x_repricer_rows_len, batches))
    batch = 0
    while batch <= batches:
        try:
            if batch % 50 == 0:
                logging.info("Finished updating batch %s of %s..." %(batch, batches))
            batch_rows = x_repricer_rows[batch * 1000: min(x_repricer_rows_len, (batch + 1) * 1000)]
            item_ids = []
            min_price_cases = ''
            max_price_cases = ''
            repricer_scheme_cases = ''
            for row in batch_rows:
                item_ids.append(row['SKU'])
                min_price_cases += "WHEN name='%s' THEN %s " %(row['SKU'], row['MinimumPrice'])
                max_price_cases += "WHEN name='%s' THEN %s " %(row['SKU'], row['MaximumPrice'])
                if row['RepricerName'] in repricer_mapping:
                    repricer_scheme_cases += "WHEN name='%s' THEN %s " %(row['SKU'], repricer_mapping[row['RepricerName']])

            query_2 = "WHERE name IN %s"
            if min_price_cases:
                query_1 = "UPDATE product_listing SET current_min_price = (CASE %s END) " %min_price_cases
                Connection.odoo_execute(query_1 + query_2, [item_ids], commit=True)
            if max_price_cases:
                query_1 = "UPDATE product_listing SET current_max_price = (CASE %s END) " %max_price_cases
                Connection.odoo_execute(query_1 + query_2, [item_ids], commit=True)
            if repricer_scheme_cases:
                query_1 = "UPDATE product_listing SET repricer_scheme_id = (CASE %s ELSE repricer_scheme_id END) " %repricer_scheme_cases
                Connection.odoo_execute(query_1 + query_2, [item_ids], commit=True)
        except Exception as e:
            logging.error(e)
            logging.error(query_1 + query_2)
        batch += 1
    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))


if __name__ == "__main__":
    main()

import simplejson
import os
import sys
import argparse
import getpass
from ebaysdk.exception import ConnectionError
from ebaysdk.trading import Connection as Trading
import opsystConnection

current_user = getpass.getuser()
this_module_name = os.path.basename(sys.modules['__main__'].__file__)
parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-test-only', action="store", dest="test_only")
args = vars(parser.parse_args())
test_only = bool(args['test_only'])
if args['test_only'] in ('0', 'False'):
    test_only = False

thisdir = os.path.abspath(os.path.dirname( __file__ ))
if test_only:
    fp = open(os.path.join(thisdir, 'test.json'), mode='r')
else:
    fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)
stores = config.get('stores').split(',')
odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)
credentials = Connection.get_store_credentials(stores)


def set_states():
    for store in stores:
        if credentials[store]['type'] != 'ebay':
            continue
        ebay_api = Trading(domain='api.ebay.com',
                           config_file=None,
                           appid=credentials[store]['application_key'],
                           devid=credentials[store]['developer_key'],
                           certid=credentials[store]['certificate_key'],
                           token=credentials[store]['auth_token'],
                           siteid='100')
        if credentials[store]['type'] == 'ebay':
            listings = Connection.odoo_execute("""
                SELECT name
                FROM product_listing
                WHERE store_id = %s AND state = 'active' and name = '272371629411'
            """, [credentials[store]['id']])

            for l in listings:
                item_dict = {
                    'ItemID': l['name'],
                    'Location': 'United States, United States',
                }

                try:
                    result = ebay_api.execute('ReviseItem', {'Item': item_dict}).dict()
                    item_id = result['ItemID']
                except ConnectionError as e:
                    errors = e.response.dict()['Errors']


if __name__ == "__main__":
    set_states()

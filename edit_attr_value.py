# # -*- coding: utf-8 -*-

import xmlrpclib
import logging

# url = "http://localhost:8069"
# db = 'auto_local'
# username = 'ajporlante@gmail.com'
# password = '123'
url = 'http://opsyst.com'
db = 'auto-2016-12-21'
username = 'i.rakhimkulov@vertical4.com'
password = 'fafa321'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='/tmp/myapp.log',
                    filemode='w')

listing_model = 'product.listing'
store_model = 'sale.store'
attr_model = 'product.item.specific.attribute'
value_model = 'product.item.specific.value'
product_model = 'product.template'
line_model = 'product.item.specific.line'


def go():
    old_val = 'Manual folding'
    new_val = 'Manual Folding'
    criteria = '='
    values_ids = rpc_search(value_model, [['name', criteria, old_val]])
    for value in values_ids:
        print value
        res = rpc_write(value_model, value, {'name': new_val})
        print res


def rpc_read(model, domain):
    return models.execute_kw(db, uid, password, model, 'search_read', [domain])


def rpc_search(model, domain):
    return models.execute_kw(db, uid, password, model, 'search', [domain])


def rpc_write(model, record_id, data):
    return models.execute_kw(db, uid, password, model, 'write', [[record_id], data])


def rpc_create(model, data):
    return models.execute_kw(db, uid, password, model, 'create', [data])


def rpc_del(model, record_id):
    return models.execute_kw(db, uid, password, model, 'unlink', [record_id])


def main():
    go()


if __name__ == "__main__":
    main()

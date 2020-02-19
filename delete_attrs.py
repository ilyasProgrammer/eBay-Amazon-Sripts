# -*- coding: utf-8 -*-

import xmlrpclib
import logging

url = "http://localhost:8069"
# db = 'auto_local'
# username = 'ajporlante@gmail.com'
# password = '123'
# url = 'http://opsyst.com'
db = 'auto-2016-12-21'
username = 'i.rakhimkulov@vertical4.com'
password = 'fafa321'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
listing_model = 'product.listing'
store_model = 'sale.store'
attr_model = 'product.item.specific.attribute'
value_model = 'product.item.specific.value'
product_model = 'product.template'
line_model = 'product.item.specific.line'


def go():
    name = 'Type'
    attr = rpc_search(attr_model, [['name', '=', name]])[0]
    lines = rpc_search(line_model, [['item_specific_attribute_id', '=', attr]])
    chunks = [lines[x:x + 100] for x in xrange(0, len(lines), 100)]
    for r in chunks:
        res = rpc_del(line_model, r)
        logging.info(r)


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

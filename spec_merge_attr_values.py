# -*- coding: utf-8 -*-

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

listing_model = 'product.listing'
store_model = 'sale.store'
attr_model = 'product.item.specific.attribute'
value_model = 'product.item.specific.value'
product_model = 'product.template'
line_model = 'product.item.specific.line'


def go():
    new_val = 'Door Handle'
    original_val = 'Door handle'
    attr = 'Type'
    attr_id = rpc_search(attr_model, [['name', '=', attr]])[0]
    new_val_id = rpc_search(value_model, [['name', '=', new_val], ['item_specific_attribute_id', '=', attr_id]])[0]
    orig_val_id = rpc_search(value_model, [['name', '=', original_val], ['item_specific_attribute_id', '=', attr_id]])[0]
    donor_lines = rpc_read(line_model, [['value_id', '=', orig_val_id]])
    for donor_line in donor_lines:
        print donor_line
        res = rpc_write(line_model, donor_line['id'], {'value_id': new_val_id})
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

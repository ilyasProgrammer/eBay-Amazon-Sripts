# -*- coding: utf-8 -*-

"""Common Odoo rpc functions and settings"""

import xmlrpclib

# url = "http://localhost:8069"
url = 'http://opsyst.com'
db = 'auto-2016-12-21'
username = 'i.rakhimkulov@vertical4.com'
password = 'fafa321'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url), allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)
print 'RPC to url:', url


def read(model, domain, fields=None):
    if fields:
        return models.execute_kw(db, uid, password, model, 'search_read', [domain], {'fields': fields})
    else:
        return models.execute_kw(db, uid, password, model, 'search_read', [domain])


def search(model, domain):
    return models.execute_kw(db, uid, password, model, 'search', [domain])


def write(model, record_id, data):
    return models.execute_kw(db, uid, password, model, 'write', [[record_id], data])


def create(model, data):
    return models.execute_kw(db, uid, password, model, 'create', [data])


def delete(model, record_id):
    return models.execute_kw(db, uid, password, model, 'unlink', [record_id])


def custom_method(model, method_name, record_ids):
    models.execute_kw(db, uid, password, model, method_name, record_ids)

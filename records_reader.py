# -*- coding: utf-8 -*-

import rpc

# res = rpc.read('product.template.listed', [['state', '=', 'active'], ['amz_min_price', '>', 0]], ['product_tmpl_id', 'amz_min_price'])
# res = rpc.read('product.template.listed', [['id', '=', '6072']], ['product_tmpl_id', 'amz_min_price'])
with open('/home/ra/amz', 'r') as f:
    model = 'sale.order'
    cnt = 0
    for r in f:
        # web_order_id = r.replace('\r', '').replace('\n', '')
        ebay_sales_record_number = r.replace('\r', '').replace('\n', '')
        # pr = rpc.search(model, [['web_order_id', '=', web_order_id]])
        pr = rpc.search(model, [['ebay_sales_record_number', '=', ebay_sales_record_number]])
        if len(pr):
            print cnt, 'Got', pr[0]
            pass
        else:
            print cnt, 'NO', ebay_sales_record_number
        cnt += 1
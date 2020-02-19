# -*- coding: utf-8 -*-

import logging
import rpc
import time

log_file = './logs/'+time.strftime("%Y-%m-%d %H:%M:%S")+' '+__file__.rsplit('/', 1)[1]+'.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s.%(msecs)03d %(levelname)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S", filemode='a')
logging.getLogger().addHandler(logging.StreamHandler())
listing_model = 'product.listing'
store_model = 'sale.store'
product_model = 'product.template'
psi_model = 'product.supplierinfo'

if __name__ == "__main__":
    psis = rpc.read(psi_model, [['name', '=', 316421]])
    for p in psis:
        listing = rpc.search(listing_model, [['product_tmpl_id', '=', p['product_tmpl_id'][0]]])
        if listing:
            rpc.write(listing_model, listing[0], {'brand_id': 37})
            print 'Updated', listing[0]
    pass

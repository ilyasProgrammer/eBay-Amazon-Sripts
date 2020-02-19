# -*- coding: utf-8 -*-

import rpc

qwes = rpc.search('sale.order', [['amount_total', '=', 0], ['has_exception', '=', True], ['state', '=', 'draft']])

for q in qwes:
    print q
    rpc.custom_method('sale.order', 'button_set_losy_routes', [q])

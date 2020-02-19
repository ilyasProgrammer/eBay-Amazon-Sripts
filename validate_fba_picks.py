# -*- coding: utf-8 -*-

import rpc

picks = rpc.read('stock.picking', [['amz_order_type', '=', 'fba'],
                                     ['state', '=', 'confirmed'],
                                     # ['state', '=', 'assigned'],
                                     ['picking_type_id', '=', 11]])
# picks = [437107]
for q in picks:
    print q['id']
    # rpc.custom_method('stock.picking', 'force_assign', [q])
    # rpc.custom_method('stock.picking', 'action_assign', [q])
    # for r in q['pack_operation_ids']:
    #     rpc.write('stock.pack.operation', r, {'qty_done': 1})
    try:
        # res = rpc.custom_method('stock.picking', 'do_new_transfer', [q['id']])
        res = rpc.custom_method('stock.picking', 'action_cancel', [q['id']])
    except Exception as e:
        pass

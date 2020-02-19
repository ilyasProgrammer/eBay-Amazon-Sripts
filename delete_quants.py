import rpc

"""Delete quants by history record (picking)"""

# po_id = 127413  # PO128641
# total_landed_cost = 6383.11
# total_cu_ft = 2252.250
name = 'PO158380'

po = rpc.read('purchase.order', [['name', '=', name]])
if not po:
    exit()
# receive_picking = rpc.read('stock.picking', [['origin', '=', po[0]['name']]])
receive_picking = rpc.read('stock.picking', [['name', '=','WH/IN/00502']])
if len(receive_picking) != 1:
    exit()
po_line_ids = rpc.read('purchase.order.line', [['order_id', '=', po[0]['id']]])
prods = []
for l in po_line_ids:
    if l['qty_received'] > 0:
        prods.append({'prod': l['product_id'][0], 'picks': [], 'dests': []})
for pr in prods:
    quants = rpc.read('stock.quant', [['product_id', '=', pr['prod']], ['location_id', '=', 1807]])
    for q in quants:
        moves = rpc.read('stock.move', [['origin', '=', po[0]['name']], ['id', 'in', q['history_ids']]])
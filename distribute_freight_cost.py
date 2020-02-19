import rpc


# po_id = 127413  # PO128641
# total_landed_cost = 6383.11
# total_cu_ft = 2252.250
name = 'PO204714'

po = rpc.read('purchase.order', [['name', '=', name]])
if not po:
    exit()
total_cu_ft = po[0]['cu_ft_total']
receive_picking = rpc.read('stock.picking', [['origin', '=', po[0]['name']], ['freight_cost', '!=', 0]])
if len(receive_picking) != 1:
    exit()
total_landed_cost = receive_picking[0]['freight_cost']
po_line_ids = rpc.read('purchase.order.line', [['order_id', '=', po[0]['id']]])
prods = []
for l in po_line_ids:
    if l['qty_received'] > 0:
        cu_ft = l['qty_received'] * (l['cu_ft_subtotal'] / l['product_qty'])
        landed_cost = ((cu_ft / total_cu_ft) * total_landed_cost) / l['qty_received']
        print l['product_id'][1] + ': %s %s' % (round(landed_cost, 2), l['qty_received'])
        prods.append({'prod': l['product_id'][1], 'cost': round(landed_cost, 2), 'picks': [], 'dests': []})
if len(po_line_ids):
    picks = rpc.read('stock.picking', [['origin', '=', po[0]['name']],
                                       ['location_id', '=', 8],
                                       ['location_dest_id', '=', 1807],
                                       ['state', '=', 'done']])
    # picks = rpc.read('stock.picking', [['origin', '=', po[0]['name']], ['location_id', '=', 13], ['state', '=', 'done']])
    for pick in picks:
        pick_lines = rpc.read('stock.pack.operation', [['picking_id', '=', pick['id']]])
        for pr in prods:
            for l in pick_lines:
                if pr['prod'] == l['product_id'][1]:
                    if pick['id'] not in pr['picks']:
                        pr['id'] = l['product_id'][0]
                        pr['picks'].append(pick['id'])
                    pr['dests'].append(l['location_dest_id'][0])
for pr in prods:
    quants = rpc.read('stock.quant', [['product_id', '=', pr['id']], ['location_id', 'in', pr['dests']]])
    pass
    for q in quants:
        # moves = rpc.read('stock.move', [['product_id', '=', pr['id']], ['origin', '=', po[0]['name']]])
        moves = rpc.read('stock.move', [['origin', '=', po[0]['name']], ['id', 'in', q['history_ids']]])
        if len(moves):
            rpc.write('stock.quant', q['id'], {'landed_cost': pr['cost'], 'cost': q['product_cost'] + pr['cost']})
            print str(pr) + str(q)
        # for m in moves:
        #     if q['id'] in m['quant_ids']:
        #         pass
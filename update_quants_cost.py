# -*- coding: utf-8 -*-

import rpc
import csv

imp_file = '/home/ra/cost.csv'
START_LINE = 2

with open(imp_file) as tsv:
    reader = csv.reader(tsv)
    locs = rpc.search('stock.location', [['usage', '=', 'internal']])
    cnt = 1
    for line in reader:
        if reader.line_num < START_LINE:
            continue
        lad = line[0].strip()
        pfg_pice = line[3].strip()
        lkq_price = line[6].strip()
        quants = rpc.read('stock.quant', [['reservation_id', '=', False],
                                          ['location_id', 'in', locs],
                                          ['cost', '=', 0],
                                          ['qty', '!=', 0],
                                          ['product_id.name', '=', lad]])
        if quants:
            for q in quants:
                try:
                    pfg_pice = float(pfg_pice)
                except:
                    pfg_pice = ''
                try:
                    lkq_price = float(lkq_price)
                except:
                    lkq_price = ''
                cost = min([pfg_pice, lkq_price])
                rpc.write('stock.quant', q['id'], {'product_cost': cost, 'cost': cost})
                print cnt, 'Updated', q['id'], cost, lad
        else:
            print cnt, 'Absent ', lad
        cnt += 1

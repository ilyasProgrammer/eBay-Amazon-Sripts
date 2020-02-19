# -*- coding: utf-8 -*-

import rpc
pols = [229580,
229582,
229586,
229166,
229169,
229173,
229180,
229183,
229184,
229185,
229187,
228797,
228975,
229041,
229043,
228637,
228638,
228639,
228640,
228562,
228565,
228568,
228571,
228574,
228577,
228580,
228584,
228587,
228591,
228592,
228596,
228597,
228601,
228602,
228606,
228607,
228611,
228612,
228616,
228617,
230082,
230079,
230383,
228581,
228588,
228593,
228598,
228603,
228608,
228613,
228619,
228618,
228620,
228621,
228622,
228623,
228931,
228932,
228933,
228934,
228935,
228969,
228970,
228971,
228972,
228973,
228974,
228976,
228977,
228978,
228979,
228980,
228981,
228982,
228987,
228988,
228989,
228990,
229197,
229199,
229200,
229201,
229202,
229204,
229205,
229206,
229207,
229209,
229211,
229212,
229214,
229217,
229220,
229224,
229227,
229230,
229234,
228625,
228628,
228631,
228634,
229090,
229089,
229086,
229084,
229082,
229080,
229078,
229077,
229076,
229075,
229073,
229072,
229070,
229068,
229067,
229066,
229064,
229063,
229061,
229059,
229056,
229055,
229053,
229050,
229048,
229045,
229042,
229040,
229039,
229038,
229037,
229036,
229035,
229034,
230077,
230078,
230070,
230075,
230069,
231200,
230076,
228796,
229176,
229177,
229178,
229179,
229186,
229189,
229188,
229190,
229192,
229191,
228795,
228794,
229194,
229193,
230081,
228539,
228542,
228545,
228547,
228549,
228552,
228554,
228556,
228919,
229579,
230083,
228784,
229581,
229585,
228533,
228534,
228535,
228536,
228537,
228538,
228540,
228541,
228543,
228544,
228546,
228548,
228550,
228551,
228553,
228555,
229195,
229196,
228918,
229566,
230382,
228674,
228673,
228672,
228671,
228669,
228667,
228665,
228664,
228663,
228662,
228657,
228656,
228655,
228654,
228653,
228652,
228651,
228650,
228649,
228648,
228647,
228646,
228645,
228643,
228644,
228793,
228530,
228531,
228532,
228642]


def main():
    # pos = rpc.search('purchase.order', [['amount_total', '=', 0], ['date_order', '>', '2018-09-24']])
    # for po_id in pos:
    #     try:
    #         po = rpc.read('purchase.order', [['id', '=', po_id]])
    #         vp = rpc.read('product.supplierinfo', [['name', '=', po[0]['partner_id'][0]], ['product_tmpl_id', '=', po[0]['product_id'][0]]])
    #         pol = rpc.read('purchase.order.line', [['id', '=', po[0]['order_line'][0]]])
    #         rpc.write('purchase.order.line', pol[0]['id'], {'price_unit': vp[0]['price']})
    #         print 'po %s pol %s prod %s price %s' % (po_id, pol[0]['id'], po[0]['product_id'][0], vp[0]['price'])
    #     except Exception as e:
    #         print e
    # pol = rpc.read('purchase.order.line', [['price_unit', '=', 0], ['create_date', '>', '2018-09-24']])
    for l_id in pols:
        try:
            l = rpc.read('purchase.order.line', [['id', '=', l_id]])[0]
            # vp = rpc.read('product.supplierinfo', [['name', '=', l['partner_id'][0]], ['product_tmpl_id', '=', l['product_id'][0]]])
            vp = rpc.read('product.template.listed', [['product_tmpl_id', '=', l['product_id'][0]]])
            rpc.write('purchase.order.line', l['id'], {'price_unit': vp[0]['vendor_cost']})
            print 'po %s pol %s prod %s old price %s new price %s' % (l['order_id'][0], l['id'], l['product_id'][0], l['price_unit'], vp[0]['vendor_cost'])
        except Exception as e:
            print e


if __name__ == "__main__":
    main()

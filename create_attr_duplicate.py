# -*- coding: utf-8 -*-

"""Some specific attributes must be duplicated. For example Location and Placement on Vehicle. If one of its present then another must exist too and must has same value."""

import logging
import rpc
import time
from random import shuffle
log_file = './logs/'+time.strftime("%Y-%m-%d %H:%M:%S")+' '+__file__.rsplit('/', 1)[1]+'.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s.%(msecs)03d %(levelname)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S", filemode='a')
logging.getLogger().addHandler(logging.StreamHandler())
listing_model = 'product.listing'
store_model = 'sale.store'
attr_model = 'product.item.specific.attribute'
value_model = 'product.item.specific.value'
product_model = 'product.template'
line_model = 'product.item.specific.line'


def go():
    psn ='Partslink Number'
    pn = 'Partlink Number'
    attr1_id = rpc.search(attr_model, [['name', '=', psn]])[0]
    attr2_id = rpc.search(attr_model, [['name', '=', pn]])[0]
    attr1_lines_recs = rpc.read(line_model, [['item_specific_attribute_id', '=', attr1_id]])
    logging.info('attr1 records: %s', len(attr1_lines_recs))
    done_attr2_lines = []
    for attr1_line in attr1_lines_recs:
        product_id = attr1_line['product_tmpl_id'][0]
        attr1_val_name = attr1_line['value_id'][1]
        # Check if attr2 got attribute with same name as attr1
        same_val = rpc.search(value_model, [['name', '=', attr1_val_name], ['item_specific_attribute_id', '=', attr2_id]])
        if not same_val:
            same_val = rpc.create(value_model, {'name': attr1_line['value_id'][1], 'item_specific_attribute_id': attr2_id})
            logging.info('New value created: %s', attr1_line['value_id'][1])
        else:
            same_val = same_val[0]
        # Check if attr1 product got attr2
        attr2_line_rec = rpc.read(line_model, [['product_tmpl_id', '=', product_id], ['item_specific_attribute_id', '=', attr2_id]])
        if not attr2_line_rec:
            attr2_line_rec = rpc.create(line_model, {'value_id': same_val, 'product_tmpl_id': product_id, 'item_specific_attribute_id': attr2_id})
            logging.info('New line created: %s %s', attr2_line_rec, same_val)
            attr2_line_id = attr2_line_rec
        elif attr2_line_rec[0]['value_id'][0] != same_val:  # got record, but with different value. Just log it and handle manually later
            attr2_val_rec = rpc.read(value_model, [['id', '=', attr2_line_rec[0]['value_id'][0]]])[0]
            attr2_val_name = attr2_val_rec['name']
            logging.warning('Different values: %s %s %s %s %s', product_id, attr1_id, attr2_id, attr1_val_name, attr2_val_name)
            attr2_line_id = attr2_line_rec[0]['id']
        else:  # Already present correct one
            logging.info('Already same values: %s %s %s', product_id, attr1_id, attr2_id)
            attr2_line_id = attr2_line_id = attr2_line_rec[0]['id']
        done_attr2_lines.append(attr2_line_id)
    # # Same for attr2 except proceeded records
    # attr2_lines_recs = rpc.read(line_model, [['item_specific_attribute_id', '=', attr2_id]])
    # logging.info('attr2 records: %s %s', attr2, len(attr2_lines_recs))
    # for attr2_line in attr2_lines_recs:
    #     if attr2_line['id'] in done_attr2_lines:
    #         continue
    #     product_id = attr2_line['product_tmpl_id'][0]
    #     attr2_val_name = attr2_line['value_id'][1]
    #     # Check if attr1 got attribute with same name as attr2
    #     same_val = rpc.search(value_model, [['name', '=', attr2_val_name], ['item_specific_attribute_id', '=', attr1_id]])
    #     if not same_val:
    #         same_val = rpc.create(value_model, {'name': attr2_line['value_id'][1], 'item_specific_attribute_id': attr1_id})
    #         logging.info('New value created: %s', attr2_line['value_id'][1])
    #     else:
    #         same_val = same_val[0]
    #     # Check if attr2 product got attr1
    #     attr1_line_rec = rpc.read(line_model, [['product_tmpl_id', '=', product_id], ['item_specific_attribute_id', '=', attr1_id]])
    #     if not attr1_line_rec:
    #         attr1_line_rec = rpc.create(line_model, {'value_id': same_val, 'product_tmpl_id': product_id, 'item_specific_attribute_id': attr1_id})
    #         logging.info('New line created: %s %s', attr1_line_rec, same_val)
    #     elif attr1_line_rec['value_id'] != same_val:  # got record, but with different value. Just log it and handle manually later
    #         attr1_val_rec = rpc.read(value_model, [['id', '=', attr1_line_rec['value_id'][0]]])
    #         attr1_val_name = attr1_val_rec['name'][1]
    #         logging.warning('Different values: %s %s %s %s %s', product_id, attr1_id, attr2_id, attr1_val_name, attr2_val_name)
    #     else:  # Already present correct one
    #         logging.info('Already same values: %s %s %s', product_id, attr1_id, attr2_id)


def go2():
    attr1 = 'Location'
    attr2 = 'Placement on Vehicle'
    attr1_id = rpc.search(attr_model, [['name', '=', attr1]])[0]
    attr2_id = rpc.search(attr_model, [['name', '=', attr2]])[0]
    # Same for attr2 except proceeded records
    attr2_lines_recs = rpc.read(line_model, [['item_specific_attribute_id', '=', attr2_id]])
    shuffle(attr2_lines_recs)
    logging.info('attr2 records: %s %s', attr2, len(attr2_lines_recs))
    for attr2_line in attr2_lines_recs:
        product_id = attr2_line['product_tmpl_id'][0]
        attr2_val_name = attr2_line['value_id'][1]
        # Check if attr1 got attribute with same name as attr2
        same_val = rpc.search(value_model, [['name', '=', attr2_val_name], ['item_specific_attribute_id', '=', attr1_id]])
        if not same_val:
            same_val = rpc.create(value_model, {'name': attr2_line['value_id'][1], 'item_specific_attribute_id': attr1_id})
            logging.info('New value created: %s', attr2_line['value_id'][1])
        else:
            same_val = same_val[0]
        # Check if attr2 product got attr1
        attr1_line_rec = rpc.read(line_model, [['product_tmpl_id', '=', product_id], ['item_specific_attribute_id', '=', attr1_id]])
        if not attr1_line_rec:
            attr1_line_rec = rpc.create(line_model, {'value_id': same_val, 'product_tmpl_id': product_id, 'item_specific_attribute_id': attr1_id})
            logging.info('New line created: %s %s', attr1_line_rec, same_val)
        elif attr1_line_rec[0]['value_id'][0] != same_val:  # got record, but with different value. Just log it and handle manually later
            attr1_val_rec = rpc.read(value_model, [['id', '=', attr1_line_rec[0]['value_id'][0]]])
            attr1_val_name = attr1_val_rec[0]['name']
            logging.warning('Different values: %s %s %s %s %s', product_id, attr1_id, attr2_id, attr1_val_name, attr2_val_name)
            same_val_attr2 = rpc.search(value_model, [['name', '=', attr2_val_name], ['item_specific_attribute_id', '=', attr2_id]])[0]
            if len(attr2_val_name) < len(attr1_val_name):
                res = rpc.write(line_model, attr2_line['id'], {'value_id': same_val_attr2})
                logging.warning('Updated different names: %s %s with %s', product_id, attr2_val_name, attr1_val_name)
            else:
                res = rpc.write(line_model, attr1_line_rec[0]['id'], {'value_id': same_val})
                logging.warning('Updated different names: %s %s with %s', product_id, attr1_val_name, attr2_val_name)
        else:  # Already present correct one
            logging.info('Already same values: %s %s %s', product_id, attr1_id, attr2_id)


def update_dif_vals():
    psn ='Partslink Number'
    pn = 'Partlink Number'
    # attr1 = 'Location'
    # attr2 = 'Placement on Vehicle'
    attr1_id = rpc.search(attr_model, [['name', '=', psn]])[0]
    attr2_id = rpc.search(attr_model, [['name', '=', pn]])[0]
    attr1_lines_recs = rpc.read(line_model, [['item_specific_attribute_id', '=', attr1_id]])
    logging.info('attr1 records: %s %s', psn, len(attr1_lines_recs))
    for attr1_line in attr1_lines_recs:
        product_id = attr1_line['product_tmpl_id'][0]
        attr1_val_name = attr1_line['value_id'][1]
        attr2_line_rec = rpc.read(line_model, [['product_tmpl_id', '=', product_id], ['item_specific_attribute_id', '=', attr2_id]])
        same_val_id = rpc.search(value_model, [['name', '=', attr1_val_name], ['item_specific_attribute_id', '=', attr2_id]])
        if not same_val_id:
            same_val_id = rpc.create(value_model, {'name': attr1_line['value_id'][1], 'item_specific_attribute_id': attr2_id})
            logging.info('New value created: %s', attr1_line['value_id'][1])
        else:
            same_val_id = same_val_id[0]
        if attr2_line_rec[0]['value_id'][0] != same_val_id:  # got record, but with different value. Just log it and handle manually later
            attr2_val_rec = rpc.read(value_model, [['id', '=', attr2_line_rec[0]['value_id'][0]]])
            attr2_val_name = attr2_val_rec[0]['name']
            logging.warning('Different values: %s %s %s %s %s', product_id, attr1_id, attr2_id, attr1_val_name, attr2_val_name)
            same_val_attr1 = rpc.search(value_model, [['name', '=', attr1_val_name], ['item_specific_attribute_id', '=', attr1_id]])[0]
            if len(attr2_val_name) < len(attr1_val_name):
                res = rpc.write(line_model, attr2_line_rec[0]['id'], {'value_id': same_val_id})
                logging.warning('Updated different names: %s %s with %s', product_id, attr2_val_name, attr1_val_name)
            else:
                res = rpc.write(line_model, attr1_line['id'], {'value_id': same_val_attr1})
                logging.warning('Updated different names: %s %s with %s', product_id, attr1_val_name, attr2_val_name)
        else:
            logging.info('Same %s %s', attr1_line['value_id'], same_val_id)


if __name__ == "__main__":
    logging.info('\n\n\nSTART %s', __file__.rsplit('/', 1)[1])
    # go()
    update_dif_vals()
    logging.info('Done !')




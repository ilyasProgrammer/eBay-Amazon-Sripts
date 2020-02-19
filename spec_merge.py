# -*- coding: utf-8 -*-

"""Moves lines of one specific attribute to another one with same values, deleting original lines."""

import logging
import rpc

logging.basicConfig(filename=__file__.rsplit('/', 1)[1]+'.log', level=logging.DEBUG, format='%(asctime)s.%(msecs)02d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S", filemode='a')
logging.getLogger().addHandler(logging.StreamHandler())
listing_model = 'product.listing'
store_model = 'sale.store'
attr_model = 'product.item.specific.attribute'
value_model = 'product.item.specific.value'
product_model = 'product.template'
line_model = 'product.item.specific.line'


def go():
    acceptor = 'Type'
    donor = 'Type2'
    attr_acceptor = rpc.search(attr_model, [['name', '=', acceptor]])[0]
    attr_donor = rpc.search(attr_model, [['name', '=', donor]])[0]
    donor_lines = rpc.read(line_model, [['item_specific_attribute_id', '=', attr_donor]])
    acceptor_lines = rpc.read(line_model, [['item_specific_attribute_id', '=', acceptor]])
    logging.info('Donor records: %s', len(donor_lines))
    logging.info('Acceptor records: %s', len(acceptor_lines))
    created_cnt = 0
    for donor_line in donor_lines:
        logging.info(donor_line)
        # Check if value exist for attr_acceptor attribute
        value_id = rpc.search(value_model, [['name', '=', donor_line['value_id'][1]], ['item_specific_attribute_id', '=', attr_acceptor]])
        if not value_id:
            value_id = rpc.create(value_model, {'name': donor_line['value_id'][1], 'item_specific_attribute_id': attr_acceptor})
            logging.info('New value created: %s', donor_line['value_id'][1])
        else:
            value_id = value_id[0]
        # Check if line exist for attribute
        line = rpc.search(line_model, [['product_tmpl_id', '=', donor_line['product_tmpl_id'][0]], ['item_specific_attribute_id', '=', attr_acceptor]])
        if line:
            logging.info('exist')
        else:
            new_line = rpc.create(line_model, {'value_id': value_id, 'product_tmpl_id': donor_line['product_tmpl_id'][0], 'item_specific_attribute_id': attr_acceptor})
            logging.info('new line %s %s', new_line,  donor_line['value_id'][1])
            created_cnt += 1
        res = rpc.delete(line_model, donor_line['id'])
        logging.info('delete %s', res)
    logging.info('Total must be: %s' % (created_cnt + len(acceptor_lines)))
    acceptor_lines = rpc.read(line_model, [['item_specific_attribute_id', '=', acceptor]])
    logging.info('Total: %s' % len(acceptor_lines))


if __name__ == "__main__":
    logging.info('START %s', __file__.rsplit('/', 1)[1])
    go()

import xmlrpclib
import logging
import csv

START_LINE = 1
# url = "http://localhost:8069"
# db = 'auto_local'
# username = 'ajporlante@gmail.com'
# password = '123'
url = 'http://opsyst.com'
db = 'auto-2016-12-21'
#username = '123@vertical4.com'
#password = '123'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

logging.basicConfig(level=logging.INFO)


def import_items():
    attr_model = 'product.item.specific.attribute'
    value_model = 'product.item.specific.value'
    product_model = 'product.template'
    line_model = 'product.item.specific.line'

    imp_file = "specificationsPFGedited.csv"

    with open(imp_file) as tsv:
        reader = csv.reader(tsv)
        for line in reader:
            if reader.line_num < START_LINE:
                continue
            name = line[2][:-1].strip()
            lad = line[0].strip()
            val = line[3].strip()
            # Attribute
            attribute = models.execute_kw(db, uid, password, attr_model, 'search', [[['name', '=', name]]])
            if not attribute:
                attribute = models.execute_kw(db, uid, password, attr_model, 'create', [{'name': name}])
                logging.info('Created item specific attribute: %s' % attribute)
            else:
                attribute = attribute[0]
            # Value
            value = models.execute_kw(db, uid, password, value_model, 'search', [[['name', '=', val], ['item_specific_attribute_id', '=', attribute]]])
            if not value:
                value = models.execute_kw(db, uid, password, value_model, 'create', [{'name': val, 'item_specific_attribute_id': attribute}])
                logging.info('Created item specific value: %s' % value)
            else:
                value = value[0]
            # Template
            template = models.execute_kw(db, uid, password, product_model, 'search', [[['part_number', '=', lad]]])
            if not template:
                logging.info('There is no product template for LAD = %s' % lad)
                continue
            else:
                template = template[0]
            # Line
            line = models.execute_kw(db, uid, password, line_model, 'search', [[['product_tmpl_id', '=', template], ['item_specific_attribute_id', '=', attribute]]])
            if line:
                logging.info('Product %s already got attribute %s' % (template, attribute))  # Or overwrite ? TODO
                continue
            line = models.execute_kw(db, uid, password, line_model, 'create', [{'value_id': value,
                                                                                'product_tmpl_id': template,
                                                                                'item_specific_attribute_id': attribute}])
            logging.info('Created new product item specific line: %s' % line)
            logging.info('Line processed: %s' % reader.line_num)


def main():
    import_items()


if __name__ == "__main__":
    main()

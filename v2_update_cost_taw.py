# -*- coding: utf-8 -*-
"""Updates costs for TAW using domesticCost field from Autoplus"""

import argparse
import rpc
import logging
import os
import sys
import simplejson
import getpass
from datetime import datetime
import opsystConnection
import slack
import socket

logging.captureWarnings(True)
logging.getLogger('requests').setLevel(logging.ERROR)
current_user = getpass.getuser()
current_host = socket.gethostname()
this_module_name = os.path.basename(sys.modules['__main__'].__file__)
parser = argparse.ArgumentParser(description='Parser')
thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)

odoo_db = config.get('odoo_db')
odoo_db_host = config.get('odoo_db_host')
odoo_db_user = config.get('odoo_db_user')
odoo_db_password = config.get('odoo_db_password')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
logs_path = config.get('logs_path')
src_path = config.get('src_path')
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')

LOG_FILE = os.path.join(logs_path + 'cost_taw_%s.log') % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)

Connection = opsystConnection.OpsystConnection(odoo_db=odoo_db, odoo_db_host=odoo_db_host, odoo_db_user=odoo_db_user, odoo_db_password=odoo_db_password)


def go_taw():
    vendor_code = 316421  # TAW
    qry = """SELECT Inv.PartNo AS LAD ,inv.MfgLabel, Inv.InventoryID, Inv_.MfgID, Inv_.PartNo AS ALT, coalesce(P.Cost,0) as Cost
             FROM AutoPlus.dbo.Inventory Inv
             LEFT JOIN AutoPlus.dbo.InventoryAlt Alt ON Alt.InventoryID = Inv.InventoryID
             LEFT JOIN AutoPlus.dbo.Inventory Inv_ ON Inv_.InventoryID = Alt.InventoryIDAlt
             LEFT JOIN AutoPlus.dbo.InventoryMiscPrCur P ON P.InventoryID = Inv_.InventoryID
             WHERE Inv_.MfgID = 48"""
    ap_lines = Connection.autoplus_execute(qry)
    for l in ap_lines:
        qry = """SELECT ps.id, ps.name as vendor, ps.product_code, pt.name as LAD 
                 FROM product_supplierinfo ps 
                 LEFT JOIN product_template pt 
                 ON ps.product_tmpl_id = pt.id
                 WHERE ps.name = %s AND ps.product_code = '%s' AND pt.name = '%s'""" % (vendor_code, l['ALT'], l['LAD'])
        got_line = Connection.odoo_execute(qry)
        if got_line:
            write_date = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
            if len(got_line) == 1:
                qry = """UPDATE product_supplierinfo SET price = %s, write_date = '%s' WHERE id = %s """ % (float(l['Cost']), write_date, got_line[0]['id'])
                res = Connection.odoo_execute(qry, commit=True)
                logging.info('Updated len = 1 id = %s new price=%s', got_line[0]['id'], l['Cost'])
            else:
                qry = """UPDATE product_supplierinfo SET price = %s, write_date = '%s' WHERE id = %s """ % (float(l['Cost']), write_date, got_line[0]['id'])
                res = Connection.odoo_execute(qry, commit=True)
                logging.info('Updated len > 1 %s', got_line[0]['id'])
                ids = list_to_sql_str(set([r['id'] for r in got_line])-set([got_line[0]['id']]))
                qry = """DELETE FROM product_supplierinfo ps
                         WHERE id in %s""" % ids
                res = Connection.odoo_execute(qry, commit=True)
                logging.info('Deleted %s', ids)
        else:
            qry = """SELECT id FROM product_template WHERE name = '%s'""" % l['LAD']
            res = Connection.odoo_execute(qry)
            if not res:
                logging.info('No product %s %s', l['LAD'], l['ALT'])
                new_prod = rpc.create('product.template', {'name': l['LAD'],
                                                           'part_number': l['LAD'],
                                                           'mfg_label': l['MfgLabel'],
                                                           'inventory_id': l['InventoryID'],
                                                           'description_sale': 'created by v2_update_cost_taw.py'})
                product_id = new_prod
                if product_id:
                    logging.info('Product created: %s', product_id)
                    rpc.custom_method('product.template', 'button_sync_with_autoplus', [product_id])
                    logging.info('Product synced: %s', product_id)
                else:
                    logging.error('Error creating new product')
                    continue
            else:
                product_id = res[0]['id']
            create_date = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
            qry = """INSERT INTO product_supplierinfo 
                          (name, product_code, product_tmpl_id, price, delay, currency_id, min_qty, create_date, write_date, write_uid, create_uid) 
                     VALUES (%s, '%s', %s, %s, 0, 3, 0, '%s', '%s', 1, 1) """ % (vendor_code, l['ALT'], product_id, float(l['Cost']), create_date, create_date)
            res = Connection.odoo_execute(qry, commit=True)
            logging.info('Created %s %s', l['ALT'], float(l['Cost']))


def list_to_sql_str(data_list):
    qry = ''
    for el in data_list:
        qry += "" + str(el) + " ,"
    qry = '(' + qry[:-1] + ")"
    return qry


def main():
    slack.notify_slack(this_module_name, "Started cron: %s" % this_module_name)
    go_taw()


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")

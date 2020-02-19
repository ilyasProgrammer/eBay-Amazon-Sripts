# -*- coding: utf-8 -*-

"""
- Remove "Do not reprice" from listings that out of stock
- Set keep_manual = False if manual_price > comp_price or manual_price < min_ebay_cost
"""

import argparse
import logging
import os
import sys
import simplejson
from datetime import datetime
import opsystConnection
import slack

logging.getLogger('requests').setLevel(logging.ERROR)
this_module_name = os.path.basename(sys.modules['__main__'].__file__)
thisdir = os.path.abspath(os.path.dirname(__file__))
fp = open(os.path.join(thisdir, 'stores.json'), mode='r')
config = simplejson.load(fp)
slack_bot_token = config.get('slack_bot_token')
slack_channel_id = config.get('slack_channel_id')
slack_crons_channel_id = config.get('slack_crons_channel_id')
stores = config.get('stores').split(',')

LOG_FILE = os.path.join('/mnt/vol/log/cron/manage_listings_%s.log') % datetime.today().strftime('%Y_%m')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)
slackHandler = slack.SlackHandler(level=logging.INFO, module_name=this_module_name, slack_bot_token=slack_bot_token, slack_crons_channel_id=slack_crons_channel_id)
logging.getLogger().addHandler(slackHandler)

Connection = opsystConnection.OpsystConnection(odoo_db=config.get('odoo_db'), odoo_db_host=config.get('odoo_db_host'), odoo_db_user=config.get('odoo_db_user'), odoo_db_password=config.get('odoo_db_password'))
credentials = Connection.get_store_credentials(stores)


def uncheck_keep_manual():
    logging.info('uncheck_keep_manual ... ')
    # 24-08-2018 Ilyas [8:23 PM]
    # So I think I will add 2 new fields into listing. Manual price and check box Manual price active. If check box is set, then it keep manual
    # price if it more than minimum and lower than competitor price. If one of this conditions is false, then it automatically unchecks Manual price
    # active box and starting from this moment reprice as usual. Correct?
    # Justin [8:23 PM]
    # Perfect! Unless of course we have the listing set to allow a loss
    for store in stores:
        logging.info("Processing %s", store)
        if credentials[store]['type'] == 'ebay':  # Proceeding only eBay
            uncheck_keep_manual_go(store)
        elif credentials[store]['type'] == 'amazon':  # UPD 06-11-2018 Decided to use same logic for Amazon
            uncheck_keep_manual_amazon_go(store)
    logging.info('uncheck_keep_manual Finished.')


def uncheck_keep_manual_go(store):
    # Simplified ebay repricers algorithm
    rates = {'visionary': 1.08,
             'visionary_mg': 1.07,
             'visionary_v2f': 1.06,
             'revive': 1.09,
             'revive_apc': 1.095,
             'rhino': 1.1,
             'ride': 1.11}
    # Justin 04-01-2018: So for visionary make it 3%, 2% for ride, and 1.5% for revive
    percents = {'visionary': 3,
                'visionary_mg': 2.9,
                # 'visionary_v2f': 3,  # competes only with C2C
                'revive': 1.5,
                'revive_apc': 1.4,
                'rhino': 0.5,
                'ride': 2}
    ebay_fees = {'visionary': 0.1051, 'revive': 0.1103, 'rhino': 0.1303, 'ride': 0.0847}
    rate_diff = 1 + (rates[store] - rates['visionary'])
    paypal_fee = 0.0215

    qry = """SELECT pl.id, PL.name,coalesce(PL.max_store_qty,0) as max_store_qty,PL.dumping_percent,PL.dumping_amount,PL.custom_label,
                       PL.current_price,PTL.wh_qty,PTL.vendor_qty,PTL.total_min_cost,PL.do_not_reprice, PL.keep_manual, PL. manual_price,
                       PL.do_not_restock,PL.ebay_reprice_against_competitors,PL.sell_with_loss,PL.sell_with_loss_type,
                       PL.sell_with_loss_percent,PL.sell_with_loss_amount,PTL.wh_sale_price,REP.type,REP.percent,
                       REP.amount,COMP.non_c2c_comp_price,COMP.c2c_comp_price
                FROM product_template_listed PTL
                LEFT JOIN product_listing PL
                ON PTL.product_tmpl_id = PL.product_tmpl_id
                LEFT JOIN repricer_scheme REP
                ON REP.id = PL.repricer_scheme_id
                LEFT JOIN ( SELECT COMP.product_tmpl_id,
                                   MIN(CASE WHEN COMP.seller <> 'classic2currentfabrication' THEN COMP.price ELSE NULL END) AS non_c2c_comp_price,
                                   MIN(CASE WHEN COMP.seller = 'classic2currentfabrication' THEN COMP.price ELSE NULL END) AS c2c_comp_price
                            FROM repricer_competitor COMP
                            WHERE COMP.state = 'active' AND COMP.price > 0
                            GROUP BY COMP.product_tmpl_id
                           ) AS COMP
                ON COMP.product_tmpl_id = PTL.product_tmpl_id WHERE PL.store_id = %s 
                AND PL.state = 'active' AND PL.keep_manual = TRUE""" % credentials[store]['id']
    listings = Connection.odoo_execute(qry)
    for l in listings:
        qty = -1
        if not (l['do_not_restock'] and l['custom_label'] and l['custom_label'].startswith('X-')):
            qty = 0
            if not l['total_min_cost']:
                qty = 0
            elif l['wh_qty'] > 0 and l['vendor_qty'] > 0:
                qty = l['wh_qty'] + l['vendor_qty']
            elif l['wh_qty'] > 0:
                qty = l['wh_qty']
            elif l['vendor_qty'] > 0:
                qty = l['vendor_qty']
            if int(l['max_store_qty']):  # Individual listing limit has higher priority
                if qty > int(l['max_store_qty']):
                    qty = int(l['max_store_qty'])
            elif qty > credentials[store]['max_qty']:
                qty = credentials[store]['max_qty']
        price = -1
        if not l['do_not_reprice'] and qty > 0 and l['total_min_cost'] > 0:
            ebay_fee = ebay_fees[store] if store in ebay_fees else 0.11
            min_ebay_cost = (0.03 + l['total_min_cost']) / (1 - ebay_fee - paypal_fee)
            if l['sell_with_loss']:
                if l['sell_with_loss_type'] == 'percent':
                    percent = l['sell_with_loss_percent'] if l['sell_with_loss_percent'] > 0 else 1
                    min_ebay_cost = (100 - percent) / 100 * min_ebay_cost
                else:
                    amount = l['sell_with_loss_amount'] if l['sell_with_loss_amount'] > 0 else 1
                    min_ebay_cost = min_ebay_cost - amount
            price = min_ebay_cost * rates[store]
            if l['custom_label'] and l['custom_label'].startswith('X-'):
                price = -1
            elif l['custom_label'] and l['custom_label'].startswith('MG-'):
                if l['wh_qty'] > 0 and l['wh_sale_price'] > 0:
                    price = max((1 + (rates['visionary_mg'] - rates['visionary'])) * l['wh_sale_price'], min_ebay_cost)
                else:
                    price = min_ebay_cost * rates['visionary_mg']
            elif l['custom_label'] and l['custom_label'].startswith('V2F'):
                if l['wh_qty'] > 0 and l['wh_sale_price'] > 0:
                    price = max((1 + (rates['visionary_v2f'] - rates['visionary'])) * l['wh_sale_price'], min_ebay_cost)
                else:
                    price = min_ebay_cost * rates['visionary_v2f']
            elif l['custom_label'] and l['custom_label'].startswith('APC'):
                if l['wh_qty'] > 0 and l['wh_sale_price'] > 0:
                    price = max((1 + (rates['revive_apc'] - rates['visionary'])) * l['wh_sale_price'], min_ebay_cost)
                else:
                    price = min_ebay_cost * rates['revive_apc']
            else:
                # Dont pull down price of wh available listing
                if price > 0 and l['wh_qty'] > 0:
                    if l['wh_sale_price'] > 0:
                        price = max(rate_diff * l['wh_sale_price'], min_ebay_cost)
                    elif l['current_price'] > 0:
                        price = max(l['current_price'], min_ebay_cost)
            if price > 0 and l['ebay_reprice_against_competitors'] and (l['non_c2c_comp_price'] > 0 or l['c2c_comp_price'] > 0):
                comp_price = l['non_c2c_comp_price']
                if store == 'rhino' or (l['custom_label'] and l['custom_label'].startswith('V2F')):
                    comp_price = l['c2c_comp_price']
                if l['keep_manual'] and l['manual_price'] > 0 and comp_price > 0:
                    if min_ebay_cost < l['manual_price'] < comp_price:
                        price = l['manual_price']
                    else:
                        Connection.odoo_execute("""update product_listing set keep_manual = False where id = %s""" % l['id'], commit=True)
                        logging.info('Keep manual price unchecked for listing: %s', l['id'])


def uncheck_keep_manual_amazon_go(store):
    rows = Connection.odoo_execute("""
           SELECT PL.id, PL.name, PTL.wh_qty, PL.current_min_price, PL.current_max_price, PL.keep_manual, PL. manual_price, 
           REP.name as repricer, PTL.total_min_cost FROM product_template_listed PTL
           LEFT JOIN product_listing PL ON PL.product_tmpl_id = PTL.product_tmpl_id
           LEFT JOIN repricer_scheme REP on REP.id = PL.repricer_scheme_id
           WHERE PL.store_id = %s AND PL.state = 'active'
           AND (PL.do_not_reprice = False OR PL.do_not_reprice IS NULL) AND PL.name NOT LIKE 'WHCO-%%' AND (PTL.wh_qty > 0 OR PTL.vendor_qty > 0)
           AND PTL.total_min_cost > 0
       """, [credentials[store]['id']])

    if rows:
        for row in rows:
            min_price = 1.137 * row['total_min_cost']  # Mainly intended for dropship
            if row['name'].startswith('MAPM'):
                repricer = 'MAPM listings'
                if row['name'].startswith('MAPM-PBL'):
                    min_price = 1.5 * row['total_min_cost']
                else:
                    min_price = 1.45 * row['total_min_cost']
            if row['keep_manual']:
                if min_price > row['manual_price']:
                    Connection.odoo_execute("""update product_listing set keep_manual = False where id = %s""" % row['id'], commit=True)
                    logging.info('Keep manual price unchecked for listing: %s', l['id'])


def uncheck_do_not_reprice():
    logging.info('uncheck_do_not_reprice ... ')
    # 19-08-2018 Waqar Ali Khan [6:50 PM]
    # For example if we have an item and we mark it as Do not reprice and set a price our system do not change the price as per our daily feed.
    # and we do it for WH items only once the the item is out of stock the do not reprice option should remove automatically and once it comes
    # back in stock we must need to adjust price as per new stock
    Connection.odoo_execute("""UPDATE product_listing
                                  SET do_not_reprice = FALSE
                                  WHERE id IN (SELECT pl.id
                                               FROM product_listing pl
                                               WHERE pl.product_tmpl_id IN (SELECT ptl.product_tmpl_id
                                                                            FROM product_template_listed ptl
                                                                            WHERE state='active' AND wh_qty = 0)
                                               AND pl.do_not_reprice = TRUE
                                               AND pl.state = 'active')""", commit=True)
    logging.info('Removed "Do not reprice" from listings that out of stock')


def main():
    uncheck_keep_manual()
    uncheck_do_not_reprice()


if __name__ == "__main__":
    logging.info("Started ...")
    main()
    logging.info("Finished.\n")


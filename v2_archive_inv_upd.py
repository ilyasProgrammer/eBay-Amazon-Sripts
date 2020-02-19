# -*- coding: utf-8 -*-

import os
from datetime import datetime
import tarfile
import shutil
import logging
import getpass
import sys
import slack

current_user = getpass.getuser()
logging.getLogger('requests').setLevel(logging.ERROR)
this_module_name = os.path.basename(sys.modules['__main__'].__file__)
LOG_FILE = '/mnt/vol/log/cron/archive_inv_upd.log'
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=LOG_FILE, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(sh)

path = '/mnt/vol/misc/inv_updates/'
arch_path = '/mnt/vol/arch/inv_updates/'


def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


if __name__ == '__main__':
    logging.info('Started inv updates archiving script ...')
    slack.notify_slack(this_module_name, "Started cron: %s at %s" % (this_module_name, datetime.utcnow()))
    now = datetime.now()
    now_str = now.strftime('%Y_%m_%d')

    for root, dirnames, filenames in os.walk(path):
        for dirnam in dirnames:
            dir_day = datetime.strptime(dirnam, '%Y_%m_%d')
            if (now - dir_day).days > 5:
                logging.info('Archiving: %s ...', os.path.join(arch_path, dirnam + '.tar.gz'))
                make_tarfile(os.path.join(arch_path, dirnam + '.tar.gz'), os.path.join(path, dirnam))
                logging.info('Archived: %s', os.path.join(arch_path, dirnam + '.tar.gz'))
                shutil.rmtree(os.path.join(path, dirnam))
                logging.info('Deleted: %s', os.path.join(path, dirnam))
            else:
                logging.info('Still keep: %s', dirnam)

    slack.notify_slack(this_module_name, "Ended cron: %s at %s" % (this_module_name, datetime.utcnow()))
    logging.info('Finished.\n')

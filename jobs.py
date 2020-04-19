import time
import logging

from psycopg2 import OperationalError

import db

logger = logging.getLogger(__name__)

def log_message_action(message, stored):
    s = 'stored: ' if stored else 'ignored:'
    logger.debug('{} {}'.format(s, message.timestamp))


def commit_message(pool, message):
    while True:
        with db.get_conn(pool) as connection:
            try:
                stored = db.store_message(connection, message)
                log_message_action(message, stored)
                break
            except OperationalError:
                time.sleep(1)
                continue

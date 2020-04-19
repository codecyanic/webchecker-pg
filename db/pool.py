import time
from psycopg2 import OperationalError

SLEEP_INTERVAL = 10


class get_conn:
    def __init__(self, pool):
        self.pool = pool
        self.conn = None

    def __enter__(self):
        while True:
            try:
                self.conn = self.pool.getconn()
                return self.conn
            except OperationalError:
                time.sleep(SLEEP_INTERVAL)

    def __exit__(self, type, value, traceback):
        self.pool.putconn(self.conn)

#!/usr/bin/env python

import time
import sqlite3
import os
import copy
import BaseHTTPServer
import logging
from urlparse import parse_qs
from threading import Lock, Thread
from functools import wraps

CACHE_SIZE = 1000000  # 1 MB
TRIGGER_AGE = 8  # 8 sec


def get_logger(level=logging.INFO, filename='/tmp/appcues-log'):
    print "Please >tail -f {filename} to see logs. You can also change log level (dafult:INFO)"
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT, level=level, filename=filename)
    logger=logging.getLogger('Appcues-challenge')
    return logger
logger = get_logger()


def run_async(func):
    @wraps(func)
    def async_func(*args, **kwargs):
        func_hl = Thread(target=func, args=args, kwargs=kwargs)
        func_hl.start()
        return func_hl

    return async_func


shutdown_hook=False


class DbManager:
    def __init__(self, db_path='numbers.db'):
        self.db_path = db_path
        if self.is_db():
            message = 'Path {path} is not empty, enter any key to remove it'.format(path=db_path)
            logger.info(message)
            raw_input(message)
            self.rm_db()
            logger.info("Database removed")
        self.create_db()
        logger.info("Database created")

    def create_db(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()
        self.cur.execute('''CREATE TABLE numbers (key TEXT, value INTEGER DEFAULT 0);''')
        self.cur.execute('''CREATE UNIQUE INDEX numbers_key_index ON numbers (key);''')
        logger.info("Database tables created!")
        self.conn.commit()

    def is_db(self):
        return os.path.isdir(self.db_path) or os.path.isfile(self.db_path)

    def rm_db(self):
        os.remove(self.db_path)

    def execute(self, statements):
        logger.debug("Executing following statement: %s", statements)
        # TODO: check for SQL injection
        self.cur.execute(statements)
        self.conn.commit()

    def close(self):
        self.conn.close()


class InMemStore:
    key_val_store = {}
    store_lock = Lock()
    mem_size = 0 # in bytes
    age = time.time()

    def __init__(self):
        pass

    def increment(self, key, val):
        self.store_lock.acquire()
        logger.debug("Incrementing key: %s with value: %s", key, val)
        try:
            if key not in self.key_val_store:
                self.key_val_store[key] = 0
                self.mem_size += len(key)+32 #total bytes (max)
            self.key_val_store[key] += val
        finally:
            self.store_lock.release()

    def get_size(self):
        return self.mem_size

    def reset_size(self):
        self.mem_size = 0

    def flush_to_sql_statements(self):
        self.store_lock.acquire()
        try:
            tmp_store = self.key_val_store
            self.key_val_store = {}
            self.reset_age()
            self.reset_size()
        finally:
            self.store_lock.release()
        sql_stmt = InMemStore.get_store_as_sql(tmp_store)
        if len(tmp_store):
            return sql_stmt
        else: 
            None

    def get_age(self):
        return time.time() - self.age

    def reset_age(self):
        self.age=time.time()

    @staticmethod
    def get_store_as_sql(store):
        stmt = '''INSERT OR REPLACE INTO numbers (key, value) VALUES {tuples};'''
        one_tuple = '''('{key}', IFNULL((SELECT value FROM numbers WHERE key = '{key}'),0)+{value})'''
        tuples = ','.join([one_tuple.format(key=k, value=v) for k, v in store.iteritems()])
        return stmt.format(tuples=tuples)


@run_async
def start_manager():
    db = DbManager()
    while True:
        if store.get_age() > TRIGGER_AGE or store.get_size() >= CACHE_SIZE or shutdown_hook:
            logger.info("Flushing local cache to database. Cache size: {bytes} bytes maximum.".format(bytes=store.get_size()))
            sql_stmt = store.flush_to_sql_statements()
            if sql_stmt:
                db.execute(sql_stmt)

        if shutdown_hook:
            logger.info("Shutting down database.")
            db.close()
            break
        time.sleep(1)    


class AppcuesServer(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_POST(self):
        if 'Content-Length' not in self.headers:
            # https://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.4
            self.send_response(411)
            return
        if self.path != '/increment':
            self.send_response(400)
            return
        data_length = self.headers['Content-Length']
        data_length = int(data_length)
        data = self.rfile.read(data_length)
        content = parse_qs(data)
        keys = content.keys()
        if 'key' not in keys or 'value' not in keys:
            # bad request
            self.send_response(400)
            return
        key, value = content['key'][0], content['value'][0]
        try:
            value = int(value)
        except ValueError:
            self.send_response(400)
            return
        store.increment(key, value)
        self.send_response(200)       

    def log_message(self, format, *args):
        logger.debug(args)

if __name__ == '__main__':
    server_port = 3333
    server = BaseHTTPServer.HTTPServer(('', server_port), AppcuesServer)
    print "Starting Server on port {port}".format(port=server_port)
    try:
        store = InMemStore()
        start_manager()
        server.serve_forever()
    except KeyboardInterrupt:
        print "Shutting down the server"
        server.socket.close()
        server.shutdown()
        shutdown_hook = True


#!/usr/bin/env python

import time
import sqlite3
import os
import copy
import BaseHTTPServer
from urlparse import parse_qs
from threading import Lock, Thread
from functools import wraps

CACHE_SIZE = 1000000  # 1 MB
AGE_TRIGGER = 8  # 8 sec


def run_async(func):
    @wraps(func)
    def async_func(*args, **kwargs):
        func_hl = Thread(target=func, args=args, kwargs=kwargs)
        func_hl.start()
        return func_hl

    return async_func


shutdown_hook=False

class DbManager:
    def __init__(self, cache, db_path='numbers.db'):
        self.db_path = db_path
        if self.is_db():
            raw_input('Path {path} is not empty, enter any key to remove it'.format(path=db_path))
            self.rm_db()
        self.cache = cache
        self.cache.register_expiry_callback(self.flush)

    def create_db(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()
        self.cur.execute('''CREATE TABLE numbers (key TEXT, value INTEGER DEFAULT 0);''')
        self.cur.execute('''CREATE UNIQUE INDEX numbers_key_index ON numbers (key);''')
        self.conn.commit()

    def is_db(self):
        return os.path.isdir(self.db_path) or os.path.isfile(self.db_path)

    def rm_db(self):
        os.remove(self.db_path)

    def close(self):
        self.conn.close()

    def flush(self):
        '''write cache to db'''
        self.is_db() or self.create_db()
        sql_stmp = self.cache.flush()
        print sql_stmp
        if sql_stmp:
            self.cur.execute(sql_stmp)
            self.conn.commit()

        global shutdown_hook
        if shutdown_hook:
            self.close()


class InMemStore:
    key_val_store = {}
    store_lock = Lock()
    mem_size = 0
    age = time.time()

    def __init__(self):
        pass

    def put(self, key, val):
        if key in self.key_val_store:
            self.mem_size -= len(key) - len(str(self.key_val_store[key]))
        self.store_lock.acquire()
        try:
            self.key_val_store[key] = val
        finally:
            self.store_lock.release()
        self.mem_size += len(key) + len(str(val))

    def get_size(self):
        return self.mem_size

    def flush(self):
        self.store_lock.acquire()
        try:
            # tmp_store = copy.copy(self.key_val_store)
            tmp_store = self.key_val_store

            self.key_val_store = {}
            self.age = time.time()
            self.mem_size = 0
        finally:
            self.store_lock.release()
        sql_stmt = InMemStore.get_store_as_sql(tmp_store)
        return sql_stmt

    def get_age(self):
        return time.time() - self.age

    @staticmethod
    def get_store_as_sql(store):
        stmt = '''INSERT OR REPLACE INTO 'numbers' ('key', 'value') VALUES {tuples} ;'''
        tuples = ','.join(["('"+k+"', "+str(v)+")" for k, v in store.iteritems()])
        return None if store == {} else stmt.format(tuples=tuples)

    @run_async
    def register_expiry_callback(self, func):
        global shutdown_hook
        while True:
            if self.get_age() > AGE_TRIGGER or self.get_size() >= CACHE_SIZE or shutdown_hook:
                func()
            if shutdown_hook:
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
        # print key, value
        self.store.put(key, value)
        self.send_response(200)       

    def log_message(self, format, *args):
        return

    store = InMemStore()
    db = DbManager(store)

if __name__ == '__main__':
    server_port = 3333
    server = BaseHTTPServer.HTTPServer(('', server_port), AppcuesServer)
    print "Starting Server on port {port}".format(port=server_port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print "Shutting down the server"
        server.socket.close()
        shutdown_hook = True
        server.shutdown()


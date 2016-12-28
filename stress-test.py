#!/usr/bin/env python
import random
import Queue
import requests
import sys
import getopt
from threading import Thread
from time import time

queue = Queue.Queue()


def worker():
    while True:
        item = queue.get()
        key = item[0]
        values = item[1]
        for value in values:
            requests.post('http://localhost:3333/increment', data={'key': key, 'value': value})

        queue.task_done()


def load_words_in_queue(total_keys=100, values_per_key=10, value_range=1000):
    print "Total {total_keys} keys will be generated. Each key will have {values_per_key} keys.".format(total_keys=100, values_per_key=10)
    print "Total {total_hits} POST calls will be made to the server.".format(total_hits=total_keys*values_per_key)
    lines = open('/usr/share/dict/words').read().splitlines()

    total_values = 0
    for i in range(total_keys):
        key = random.choice(lines)
        values = [random.choice(range(value_range)) for i in range(values_per_key)]
        queue.put(
            [key, values]
        )
        total_values += sum(values)
    return total_values


if __name__ == '__main__':
    print "generating keys and values, it will take a while."
    total_values = load_words_in_queue()
    print "Sum of all values in the database should be:", total_values
    print "keys and values generated. Starting to hit server. Make sure server is up"
    worker_threads = 10
    start_time = time()
    for i in range(worker_threads):
        t = Thread(target=worker)
        t.daemon = True
        t.start()

    queue.join()
    print "Total time taken in secs: ", time() - start_time

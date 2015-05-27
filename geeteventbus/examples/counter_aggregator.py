#!/usr/bin/env python

from threading import Lock, Thread
from time import sleep, time
from geeteventbus.eventbus import eventbus
from geeteventbus.event import event
from geeteventbus.subscriber import subscriber
from random import randint


class counter_aggregator(subscriber, Thread):
    '''
    Aggregator for a set of counters. Multiple threads updates the counts which
    are aggregated by this class and output the aggregated value periodically.
    '''
    def __init__(self, counter_names):
        Thread.__init__(self)
        self.counter_names = counter_names
        self.locks = {}
        self.counts = {}
        self.keep_running = True
        self.collect_times = {}
        for counter in counter_names:
            self.locks[counter] = Lock()
            self.counts[counter] = 0
            self.collect_times[counter] = time()

    def process(self, eobj):
        '''
        Process method calls with the event object eobj. eobj has the counter name as the topic
        and an int count as the value for the counter.
        '''
        counter_name = eobj.get_topic()
        if counter_name not in self.counter_names:
            return
        count = eobj.get_data()
        with self.locks[counter_name]:
            self.counts[counter_name] += count

    def stop(self):
        self.keep_running = False

    def __call__(self):
        '''
        Keep outputing the aggregated counts every 2 seconds
        '''
        while self.keep_running:
            sleep(2)
            for counter_name in self.counter_names:
                with self.locks[counter_name]:
                    print('Change for counter %s = %d, in last %f secs' % (counter_name,
                          self.counts[counter_name], time() - self.collect_times[counter_name]))
                    self.counts[counter_name] = 0
                    self.collect_times[counter_name] = time()
        print('Aggregator exited')


class count_producer:
    '''
    Producer for counters. Every 0.02 seconds post the "updated" value for a
    counter randomly
    '''
    def __init__(self, counters, ebus):
        self.counters = counters
        self.ebus = ebus
        self.keep_running = True
        self.num_counter = len(counters)

    def stop(self):
        self.keep_running = False

    def __call__(self):
        while self.keep_running:
            ev = event(self.counters[randint(0, self.num_counter - 1)], randint(1, 100))
            ebus.post(ev)
            sleep(0.02)
        print('producer exited')

if __name__ == '__main__':
    ebus = eventbus()
    counters = ['c1', 'c2', 'c3', 'c4']
    subcr = counter_aggregator(counters)
    producer = count_producer(counters, ebus)
    for counter in counters:
        ebus.register_consumer(subcr, counter)
    threads = []
    i = 30
    while i > 0:
        threads.append(Thread(target=producer))
        i -= 1

    aggregator_thread = Thread(target=subcr)
    aggregator_thread.start()
    for thrd in threads:
        thrd.start()
    sleep(20)
    producer.stop()
    subcr.stop()
    sleep(2)
    ebus.shutdown()

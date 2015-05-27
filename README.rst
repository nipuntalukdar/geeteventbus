geeteventbus
============
An eventbus for concurrent programming
--------------------------------------

geeteventbus is a library that allows publish-subscribe-style communication. There is no need for the components to register to each-other. It is inspired by a Java library, Guava eventbus from Google. But it is not exactly same as the Guava eventbus library.

- geeteventbus simplifies handling events from publishers and subscribers.
- publisher and subscribers don't need to create threads to concurrently process the events.
- the eventbus can be synchronus, where the events are delivered from the same thread posting the events
- events can be delivered to subscibers in the same order they are posted
- subscribers may be declared as thread-safe, in that case same subscriber may be invoked concurrently for processing multiple events
- events for which there are no subscribers are registered yet are simply discared by the eventbus.
- the eventbus is not to be used for inter process communication. Publishers and subsribers must run on the same process

Basic working
-------------

1) We create an eventbus

    .. code:: python

        from geeteventbus.eventbus import eventbus
        eb = eventbus()

   This will create an eventbus with the defaults. The default eventbus will have below characteristics:

        1) the maximum queued event limit is set to 10000
        2) number of executor thread is 8
        3) the subscribers will be called asynchronously
        4) subscibers are treated as thread-safe and hence same subscribers may be invoked simultaneously  on different threads
    
2) Create a subsclass of subscriber and override the process method. Create an object of this class and register it to the eventbus for receiving messages with certain topics:
    
    .. code:: python

        from geeteventbus.subscriber import subscriber
        from geeteventbus.eventbus import eventbus
        from geeteventbus.event import event

        class mysubscriber(subscriber):
            def process(self, eventobj):
                if not isinstance(eventobj, event):
                    print('Invalid object type is passed.')
                    return
                topic = eventobj.get_topic()
                data = eventobj.get_data()
                print('Processing event with TOPIC: %s, DATA: %s' % (topic, data))
        
        subscr = mysubscriber()
        eb.register_consumer(subscr, 'an_important_topic')

3) Post some events to the eventbus with the topic "an_important_topic".

    .. code:: python

        from geeteventbus.event import event

        eobj1 = ('an_important_topic', 'This is some data for the event 1')
        eobj2 = ('an_important_topic', 'This is some data for the event 2')
        eobj3 = ('an_important_topic', 'This is some data for the event 3')
        eobj3 = ('an_important_topic', 'This is some data for the event 4')
    
        eb.post(eobj1)
        eb.post(eobj2)
        eb.post(eobj3)
        eb.post(eobj4)

4) We may gracefully shutdown the eventbus before exiting the process

    .. code:: python
        
        eb.shutdown()


The complete example is below:
    
    .. code:: python
        
        from time import sleep
        from geeteventbus.subscriber import subscriber
        from geeteventbus.eventbus import eventbus
        from geeteventbus.event import event

        class mysubscriber(subscriber):
            def process(self, eventobj):
                if not isinstance(eventobj, event):
                    print('Invalid object type is passed.')
                    return
                topic = eventobj.get_topic()
                data = eventobj.get_data()
                print('Processing event with TOPIC: %s, DATA: %s' % (topic, data))
        
        
        eb = eventbus()
        subscr = mysubscriber()
        eb.register_consumer(subscr, 'an_important_topic')
        

        eobj1 = event('an_important_topic', 'This is some data for the event 1')
        eobj2 = event('an_important_topic', 'This is some data for the event 2')
        eobj3 = event('an_important_topic', 'This is some data for the event 3')
        eobj4 = event('an_important_topic', 'This is some data for the event 4')
    
        eb.post(eobj1)
        eb.post(eobj2)
        eb.post(eobj3)
        eb.post(eobj4)

        eb.shutdown()
        sleep(2)


A more detailed example is given below. A subscriber (counter_aggregator) aggregates the values for 
a set of counters. It registers itself to an eventbus for receiving events for the 
counters(topics). A set of producers update the values for the counters and post events describing
the counter to the eventbus:
    
    .. code:: python

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

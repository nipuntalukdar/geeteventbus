''' Main module implementing the event bus '''

from atexit import register
from time import time
from threading import Lock, Thread, current_thread
import logging
import sys
from zlib import crc32
from geeteventbus.event import event
from geeteventbus.subscriber import subscriber
PY_VER_2 = True
if sys.version[0] == '2':
    from Queue import Queue, Empty
else:
    from queue import Queue, Empty
    PY_VER_2 = False

MAX_TOPIC_INDEX = 16  # Must be power of 2
DEFAULT_EXECUTOR_COUNT = 8
MAX_EXECUTOR_COUNT = 1024
MIN_EXECUTOR_COUNT = 1
MAX_EXECUTOR_COUNT = 128
MAXIMUM_QUEUE_LENGTH = 25600
MINIMUM_QUEUE_LENGTH = 16


def get_crc32(data):
    '''Returns the crc32 value of the input string. '''

    if PY_VER_2:
        return crc32(data)
    strbytes = bytes(data, encoding='UTF-8')
    return crc32(strbytes)


class eventbus:
    def __init__(self, max_queued_event=10000, executor_count=DEFAULT_EXECUTOR_COUNT,
                 synchronus=False, subscribers_thread_safe=True):
        '''
        Creates an eventbus object

        :param max_queued_event:  total number of un-ordered events queued.
        :type max_queued_event: int
        :param executor_count:  number of threads to process the queued event by calling the
                                corresponding subscribers.
        :type executor_count: int
        :param synchronus:  if the events are processed synchronously, i.e. if they are proccessed
                            by subscribers on the same event submitting thread.
        :type synchronus: bool
        :param subscribers_thread_safe:  if the subscribers can be invoked for processing multiple
                                         events simultaneously.
        :type subscribers_thread_safe: bool
        '''

        register(self.shutdown)
        self.synchronus = synchronus
        self.subscribers_thread_safe = subscribers_thread_safe
        self.topics = MAX_TOPIC_INDEX * [{}]
        self.index_locks = []
        self.consumers = {}
        self.consumers_lock = Lock()
        self.shutdown_lock = Lock()
        self.subscriber_locks = {}
        self.keep_running = True
        self.stop_time = 0
        i = 0
        while i < MAX_TOPIC_INDEX:
            self.index_locks.append(Lock())
            i += 1

        if not self.synchronus:
            self.event_queue = Queue(max_queued_event)
            self.event_queue_size = MAXIMUM_QUEUE_LENGTH
            if max_queued_event >= MINIMUM_QUEUE_LENGTH and max_queued_event <= \
              MAXIMUM_QUEUE_LENGTH:
                self.event_queue_size = max_queued_event
            self.executor_count = executor_count
            if executor_count < MIN_EXECUTOR_COUNT or executor_count > MAX_EXECUTOR_COUNT:
                self.executor_count = DEFAULT_EXECUTOR_COUNT
            self.executors = []
            self.grouped_events = []
            self.thread_specific_queue = {}
            i = 0
            while i < self.executor_count:
                name = 'executor_thread_' + str(i)
                thrd = Thread(target=self, name=name)
                self.executors.append(thrd)
                grouped_events_queue = Queue()
                self.grouped_events.append(grouped_events_queue)
                self.thread_specific_queue[name] = grouped_events_queue
                i += 1
            for thrd in self.executors:
                thrd.start()
        else:
            def __post_synchronous(eventobj):
                topic = eventobj.get_topic()
                subscribers = self.get_subscribers(topic)
                if subscribers is not None:
                    for subscr in subscribers:
                        try:
                            subscr.process(eventobj)
                        except Exception as e:
                            logging.error(e)
            self.post_synchronous = __post_synchronous

    def post(self, eventobj):
        '''
        posts an event to the eventbus

        :param eventobj: the event posted. It must be type of event class or its subclass.
        :returns: True if event is successfully postedb, False otherwise.
        '''

        if not isinstance(eventobj, event):
            logging.error('Invalid data passed. You must pass an event instance')
            return False
        if not self.keep_running:
            return False
        if not self.synchronus:
            ordered = eventobj.get_ordered()
            if ordered is not None:
                indx = (abs(get_crc32(ordered)) & (MAX_EXECUTOR_COUNT - 1)) % self.executor_count
                queue = self.grouped_events[indx]
                queue.put(eventobj)
            else:
                self.event_queue.put(eventobj)
        else:
            self.post_synchronous(eventobj)
        return True

    def register_consumer_topics(self, consumer, topic_list):
        '''
        registers a consumer to the eventbus that subscribes to the list of topics in topic_list

        :param consumer: the subscriber object
        :param topic_list: the list of topics the consumer will subscribe for
        '''

        for topic in topic_list:
            self.register_consumer(consumer, topic)

    def register_consumer(self, consumer, topic):
        '''
        registers a consumer to the eventbus that subscribes to a topic

        :param consumer: the subscriber object
        :type consumer: subscriber subclass
        :param topic: the topic the consumer will subscribe for
        :type topic: str
        '''

        if not isinstance(consumer, subscriber):
            return False
        indexval = get_crc32(topic) & (MAX_TOPIC_INDEX - 1)
        with self.consumers_lock:
            with self.index_locks[indexval]:
                if topic not in self.topics[indexval]:
                    self.topics[indexval][topic] = [consumer]
                elif consumer not in self.topics[indexval][topic]:
                    self.topics[indexval][topic].append(consumer)
            if consumer not in self.consumers:
                self.consumers[consumer] = [topic]
            elif topic not in self.consumers[consumer]:
                self.consumers[consumer].append(topic)
            if not self.subscribers_thread_safe:
                if consumer not in self.subscriber_locks:
                    self.subscriber_locks[consumer] = Lock()

    def unregister_consumer(self, consumer):
        '''
        Unregister the consumer.

        The consumer will no longer receieve any event to process for any topic

        :param conumer: the subscriber object to unregister
        '''

        with self.consumers_lock:
            subscribed_topics = None
            if consumer in self.consumers:
                subscribed_topics = self.consumers[consumer]
                del self.consumers[consumer]
            if self.subscribers_thread_safe and (consumer in self.subscriber_locks):
                del self.subscriber_locks[consumer]

            if subscribed_topics is None:
                return
            for topic in subscribed_topics:
                indexval = get_crc32(topic) & (MAX_TOPIC_INDEX - 1)
                with self.index_locks[indexval]:
                    if (topic in self.topics[indexval]) and (consumer in
                                                             self.topics[indexval][topic]):
                        self.topics[indexval][topic].remove(consumer)
                        if len(self.topics[indexval][topic]) == 0:
                            del self.topics[indexval][topic]

    def is_subscribed(self, consumer, topic):
        '''
        Checks if a subscriber is a consumer for some topic

        :returns: True if the consumer is subscribing to the topic
        :rtype: bool
        '''
        if not isinstance(consumer, subscriber):
            logging.error('Invalid object passed')
            return False
        indexval = get_crc32(topic) & (MAX_TOPIC_INDEX - 1)
        with self.index_locks[indexval]:
            if topic not in self.topics[indexval]:
                return False
            return consumer in self.topics[indexval][topic]

    def get_subscribers(self, topic):
        '''
        Returns the list of subscribes currently registered for the topic.
        '''
        indexval = get_crc32(topic) & (MAX_TOPIC_INDEX - 1)
        with self.index_locks[indexval]:
            if topic not in self.topics[indexval]:
                return None
            ret = self.topics[indexval][topic][:]
            return ret

    def __call__(self):
        thread_specific_queue = self.thread_specific_queue[current_thread().getName()]
        fromqueue = None
        while True:
            if self.stop_time > 0:
                if time() < self.stop_time:
                    break
            eventobj = None
            try:
                if not thread_specific_queue.empty():
                    eventobj = thread_specific_queue.get()
                    fromqueue = thread_specific_queue
                else:
                    eventobj = self.event_queue.get(timeout=0.1)
                    fromqueue = self.event_queue
            except Empty as e:
                continue
            except Exception as e:
                logging.error(e)
                continue
            fromqueue.task_done()  # No harm, announce task done upfront
            topic = eventobj.get_topic()
            subscribers = self.get_subscribers(topic)
            if subscribers is not None:
                for subscr in subscribers:
                    lock = None
                    if not self.subscribers_thread_safe:
                        try:
                            lock = self.subscriber_locks[subscr]
                        except KeyError as e:
                            logging.error(e)
                            continue
                    if lock is not None:
                        lock.acquire()
                    try:
                        subscr.process(eventobj)
                    except Exception as e:
                        logging.error(e)
                    if lock is not None:
                        lock.release()

    def shutdown(self):
        '''
        Stops the event bus. The event bus will stop all its executor threads.
        It will try to flush out already queued events by calling the subscribers
        of the events. This flush wait time is 2 seconds.
        '''

        with self.shutdown_lock:
            if not self.keep_running:
                return
            self.keep_running = False
        self.stop_time = time() + 2
        if not self.synchronus:
            for thrd in self.executors:
                thrd.join()

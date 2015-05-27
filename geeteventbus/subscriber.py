''' Subscriber super-class '''

from threading import current_thread
import logging
from geeteventbus.event import event


class subscriber:

    def __init__(self):
        pass

    def process(self, eventobj):
        '''
        Called by the eventbus.

        :param eventobj: The event object
        :type eventobj: event or subclass of event

        This method implements the logic for processing the event. This method should not block for
        long time as that will affect the performance of the eventbus.
        '''
        if not self.registered:
            logging.error('Subscriber is not registered')
            return
        if not isinstance(eventobj, event):
            logging.error('Invalid object type is passed.')
            return
        print ('%s %s %s %s' % (current_thread().getName(), 'processing', eventobj.get_topic(),
               str(eventobj.get_data())))

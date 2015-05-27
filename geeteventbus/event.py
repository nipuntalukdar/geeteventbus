''' Event super class '''

import logging


class event:
    def __init__(self, topic, data, ordered=None):
        self.topic = topic
        self.data = data
        self.ordered = ordered
        if self.ordered is not None:
            if type(self.ordered) is not str:
                logging.error('Invalid type' + type(self.ordered))
                raise ValueError('Ordered field must be a string')

    def get_topic(self):
        '''
        Returns the topic associated with the topic

        :returns: the topic of the event
        :rtype: str
        '''
        return self.topic

    def get_data(self):
        '''
        Returns the data associated with the event object

        :returns: the data of the event object
        '''
        return self.data

    def get_ordered(self):
        '''
        Returns the event ordering field.

        Event ordering field may be none, specifying the events may get processed out of order
        
        :returns: the ordering field of the event object
        :rtype: str
        '''
        return self.ordered

import pika.exceptions
import mock


class MockChannel(object):

    def __init__(self):
        self.is_open = True # This mocks the original API
        self.messages = []
        self.routing_keys = []
        self.publish_counter = 0 # in incremented at every publish
        self.num_failures = 0 # Can be set before mock is called
        self.num_unroutables = 0 # Can be set before mock is called
        self.success_counter = 0

    def confirm_delivery(self): # This mocks the original API
        pass

    def basic_publish(self, *args, **kwargs): # This mocks the original API
        self.publish_counter += 1
        self.messages.append(kwargs['body'])
        self.routing_keys.append(kwargs['routing_key'])
        if self.publish_counter <= self.num_unroutables:
            raise pika.exceptions.UnroutableError('Unroutable!')
        elif self.publish_counter <= self.num_failures:
            return False
        else:
            self.success_counter += 1
            return True

class MockPikaBlockingConnection(object):

    def __init__(self, params):
        self.is_open = True # This mocks the original API
        self.host = params.host # This mocks the original API
        self.raise_channel_closed = False

        if params.host == 'please.raise.auth.error':
            raise pika.exceptions.ProbableAuthenticationError()
        elif params.host == 'please.raise.connection.error':
            raise pika.exceptions.AMQPConnectionError()
        else:
            pass

    def channel(self): # This mocks the original API
        if self.raise_channel_closed:
            raise pika.exceptions.ChannelClosed()
        else:
            return MockChannel()

    def close(self): # This mocks the original API
        self.is_open = False

    def process_data_events(self): # This mocks the original API
        pass
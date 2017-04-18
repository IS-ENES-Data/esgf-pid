import pika.exceptions
import mock


class MockChannel(object):

    def __init__(self):
        self.is_open = True # This mocks the original API
        self.messages = []
        self.routing_keys = []
        self.publish_counter = 0 # is incremented at every publish
        self.num_failures = 0 # Can be set before mock is called
        self.num_unroutables = 0 # Can be set before mock is called
        self.success_counter = 0
        self.channel_number = 1 # This mocks the original API

    def confirm_delivery(self, *args, **kwargs): # This mocks the original API
        pass

    def basic_publish(self, *args, **kwargs): # This mocks the original API
        self.publish_counter += 1
        self.messages.append(kwargs['body'])
        self.routing_keys.append(kwargs['routing_key'])
        if self.publish_counter <= self.num_unroutables:
            raise pika.exceptions.UnroutableError('Unroutableeee!')
        elif self.publish_counter <= self.num_failures:
            return False
        else:
            self.success_counter += 1
            return True

    def exchange_declare(self, *args, **kwargs): # This mocks the original API
        pass

    def queue_declare(self, *args, **kwargs): # This mocks the original API
        pass

    def queue_bind(self, *args, **kwargs): # This mocks the original API
        pass

    def add_on_close_callback(self, *args, **kwargs): # This mocks the original API
        pass

    def add_on_return_callback(self, *args, **kwargs): # This mocks the original API
        pass

class MockPikaBlockingConnection(object):

    def __init__(self, params):
        self.is_open = True # This mocks the original API
        self.host = params.host # This mocks the original API
        #self.raise_channel_closed = False

        #if params.host == 'please.raise.auth.error':
        #    raise pika.exceptions.ProbableAuthenticationError()
        #elif params.host == 'please.raise.connection.error':
        #    raise pika.exceptions.AMQPConnectionError()
        #else:
        #    pass

    def channel(self): # This mocks the original API
        return MockChannel()
        #if self.raise_channel_closed:
        #    raise pika.exceptions.ChannelClosed()
        #else:
        #    return MockChannel()

    def close(self): # This mocks the original API
        self.is_open = False

    def process_data_events(self): # This mocks the original API
        pass

class MockPikaSelectConnection(object):

    def __init__(self):
        self.is_open = True
        self.is_closed = False
        self.is_closing = False # for simplicity, leave always false
        self.ioloop = mock.MagicMock()
        self.__channel = MockChannel()

    def channel(self, on_open_callback): # This mocks the original API
        return self.__channel

    def close(self, *args, **kwargs): # This mocks the original API
        self.is_open = False
        self.is_closed = True

    def add_timeout(self, seconds, to_be_called):
        to_be_called()

    def add_on_close_callback(self, callback):
        pass


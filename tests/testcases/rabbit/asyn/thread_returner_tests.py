import unittest
import logging
import sys
if sys.version[0] == '2':
    import Queue as queue
else:
    import queue as queue
import pika
import mock
import json
import esgfpid.rabbit.asynchronous.thread_returnhandler
from esgfpid.rabbit.asynchronous.exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

class ThreadReturnerTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def make_returnhandler(self, error=None):

        thread = TESTHELPERS.get_thread_mock3(error=error)

        handler = esgfpid.rabbit.asynchronous.thread_returnhandler.UnacceptedMessagesHandler(
            thread)

        return handler, thread

    # Tests

    #
    # Sending messages
    #

    def test_send_message_ok(self):

        # Preparation:
        routing_key = 'foobar'
        emergency_routing_key = esgfpid.utils.RABBIT_EMERGENCY_ROUTING_KEY
        body = '{"foo":"bar", "ROUTING_KEY":"%s"}' % routing_key
        handler, thread = self.make_returnhandler()
        # <Basic.Return(['exchange=rabbitsender_integration_tests', 'reply_code=312', 'reply_text=NO_ROUTE', 'routing_key=cmip6.publisher.HASH.cart.datasets'])>"
        frame = mock.MagicMock()
        frame.reply_text = 'NO_ROUTE'
        frame.routing_key = routing_key
        props = mock.MagicMock()

        # Run code to be tested:
        handler.on_message_not_accepted(thread._channel, frame, props, body)

        # Check result:
        thread.send_a_message.assert_called_once_with(mock.ANY)
        call_args, call_kwargs = thread.send_a_message.call_args
        expected = {"foo": "bar", "ROUTING_KEY": emergency_routing_key, "original_routing_key": routing_key}
        self.assertEqual(json.loads(call_args[0]), expected)

    def test_send_message_second_time(self):

        # Preparation:
        routing_key = 'foobar'
        emergency_routing_key = esgfpid.utils.RABBIT_EMERGENCY_ROUTING_KEY
        body = '{"foo":"bar", "ROUTING_KEY":"%s", "original_routing_key":"%s"}' % (emergency_routing_key, routing_key)
        handler, thread = self.make_returnhandler()
        # <Basic.Return(['exchange=rabbitsender_integration_tests', 'reply_code=312', 'reply_text=NO_ROUTE', 'routing_key=cmip6.publisher.HASH.cart.datasets'])>"
        frame = mock.MagicMock()
        frame.reply_text = 'NO_ROUTE'
        frame.routing_key = emergency_routing_key
        props = mock.MagicMock()

        # Run code to be tested:
        handler.on_message_not_accepted(thread._channel, frame, props, body)

        # Check result:
        thread.send_a_message.assert_not_called()

    def test_send_message_other(self):

        # Preparation:
        routing_key = 'foobar'
        emergency_routing_key = esgfpid.utils.RABBIT_EMERGENCY_ROUTING_KEY
        body = '{"foo":"bar", "ROUTING_KEY":"%s"}' % routing_key
        handler, thread = self.make_returnhandler()
        # <Basic.Return(['exchange=rabbitsender_integration_tests', 'reply_code=312', 'reply_text=NO_ROUTE', 'routing_key=cmip6.publisher.HASH.cart.datasets'])>"
        frame = mock.MagicMock()
        frame.reply_text = 'something'
        frame.routing_key = routing_key
        props = mock.MagicMock()

        # Run code to be tested:
        handler.on_message_not_accepted(thread._channel, frame, props, body)

        # Check result:
        thread.send_a_message.assert_called_once_with(mock.ANY)
        call_args, call_kwargs = thread.send_a_message.call_args
        expected = {"foo": "bar", "ROUTING_KEY": emergency_routing_key, "original_routing_key": routing_key}
        self.assertEqual(json.loads(call_args[0]), expected)


    def test_send_message_no_key(self):

        # Preparation:
        emergency_routing_key = esgfpid.utils.RABBIT_EMERGENCY_ROUTING_KEY
        body = '{"foo":"bar"}'
        handler, thread = self.make_returnhandler()
        # <Basic.Return(['exchange=rabbitsender_integration_tests', 'reply_code=312', 'reply_text=NO_ROUTE', 'routing_key=cmip6.publisher.HASH.cart.datasets'])>"
        frame = mock.MagicMock()
        frame.reply_text = 'NO_ROUTE'
        frame.routing_key = 'whatever'
        props = mock.MagicMock()

        # Run code to be tested:
        handler.on_message_not_accepted(thread._channel, frame, props, body)

        # Check result:
        thread.send_a_message.assert_called_once_with(mock.ANY)
        call_args, call_kwargs = thread.send_a_message.call_args
        expected = {"foo": "bar", "ROUTING_KEY": emergency_routing_key, "original_routing_key": "None"}
        self.assertEqual(json.loads(call_args[0]), expected)


    def test_send_message_error(self):

        # Preparation:
        emergency_routing_key = esgfpid.utils.RABBIT_EMERGENCY_ROUTING_KEY
        body = '{"foo":"bar"}'
        handler, thread = self.make_returnhandler(error=pika.exceptions.ChannelClosed(reply_code=0, reply_text='Channel Closed'))
        # <Basic.Return(['exchange=rabbitsender_integration_tests', 'reply_code=312', 'reply_text=NO_ROUTE', 'routing_key=cmip6.publisher.HASH.cart.datasets'])>"
        frame = mock.MagicMock()
        frame.reply_text = 'NO_ROUTE'
        frame.routing_key = 'whatever'
        props = mock.MagicMock()

        # Run code to be tested:
        handler.on_message_not_accepted(thread._channel, frame, props, body)

        # Check result:
        # Can not check if error was raised...
        thread.send_a_message.assert_called_once_with(mock.ANY)
        call_args, call_kwargs = thread.send_a_message.call_args
        expected = {"foo": "bar", "ROUTING_KEY": emergency_routing_key, "original_routing_key": "None"}
        self.assertEqual(json.loads(call_args[0]), expected)

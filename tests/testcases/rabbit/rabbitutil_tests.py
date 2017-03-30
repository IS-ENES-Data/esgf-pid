import unittest
import logging

import esgfpid.defaults
import esgfpid.rabbit.rabbitutils as rutils

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

LOGGER_TO_PASS = logging.getLogger('utils')
LOGGER_TO_PASS.addHandler(logging.NullHandler())


'''
Unit tests for esgfpid.rabbit.rabbitutils.

This module does not need any other module to
function, so we don't need to use or to mock any
other objects. 
'''
class RabbitUtilsTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)


    def tearDown(self):
        LOGGER.info('#############################')

    #
    # Test getting routing key and string message
    #

    def test_get_message_and_routing_key_string_ok(self):
        
        # Test variables:
        passed_message = '{"bla":"foo", "ROUTING_KEY":"roukey"}'
        LOGGER.info('Message: %s' % passed_message)

        # Run code to be checked:
        received_key, received_message = rutils.get_routing_key_and_string_message_from_message_if_possible(passed_message)

        # Check result:
        received_message = received_message.replace("'", '"')
        expected_message = str(passed_message).replace("'", '"')
        self.assertEquals(received_key, 'roukey', 'Wrong routing key: %s' % received_key)
        self.assertEquals(expected_message, received_message, 'Wrong message.\nExpected: %s\nReceived: %s' % (expected_message, received_message))

    def test_get_message_and_routing_key_string_no_key(self):
        
        # Test variables:
        passed_message = '{"bla":"foo", "no_key":"no_key"}'
        LOGGER.info('Message: %s' % passed_message)

        # Run code to be checked:
        received_key, received_message = rutils.get_routing_key_and_string_message_from_message_if_possible(passed_message)

        # Check result:
        received_message = received_message.replace("'", '"')
        expected_message = str(passed_message).replace("'", '"')
        expected_key = esgfpid.defaults.RABBIT_DEFAULT_ROUTING_KEY
        self.assertEquals(received_key, expected_key, 'Wrong routing key: %s' % received_key)
        self.assertEquals(expected_message, received_message, 'Wrong message.\nExpected: %s\nReceived: %s' % (expected_message, received_message))

    def test_get_message_and_routing_key_none_error(self):
        
        # Test variables:
        passed_message = None
        LOGGER.info('Message: %s' % passed_message)

        # Run code to be checked:
        with self.assertRaises(ValueError):
            received_key, received_message = rutils.get_routing_key_and_string_message_from_message_if_possible(passed_message)

    def test_get_message_and_routing_key_characters(self):
        
        # Test variables:
        passed_message = 'foo'
        LOGGER.info('Message: %s' % passed_message)

        # Run code to be checked:
        received_key, received_message = rutils.get_routing_key_and_string_message_from_message_if_possible(passed_message)

        # Check result:
        received_message = received_message.replace("'", '"')
        expected_message = str(passed_message).replace("'", '"')
        expected_key = esgfpid.defaults.RABBIT_DEFAULT_ROUTING_KEY
        self.assertEquals(received_key, expected_key, 'Wrong routing key: %s' % received_key)
        self.assertEquals(expected_message, received_message, 'Wrong message.\nExpected: %s\nReceived: %s' % (expected_message, received_message))

    def test_get_message_and_routing_key_json_ok(self):
        
        # Test variables:
        passed_message = {"bla":"foo", "ROUTING_KEY":"roukey"}
        LOGGER.info('Message: %s' % passed_message)

        # Run code to be checked:
        received_key, received_message = rutils.get_routing_key_and_string_message_from_message_if_possible(passed_message)

        # Check result:
        received_message = received_message.replace("'", '"')
        expected_message = str(passed_message).replace("'", '"')
        self.assertEquals(received_key, 'roukey', 'Wrong routing key: %s' % received_key)
        self.assertEquals(expected_message, received_message, 'Wrong message.\nExpected: %s\nReceived: %s' % (expected_message, received_message))

    def test_get_message_and_routing_key_json_no_rk(self):
        
        # Test variables:
        passed_message = {"bla":"foo", "no_key":"no_key"}
        LOGGER.info('Message: %s' % passed_message)

        # Run code to be checked:
        received_key, received_message = rutils.get_routing_key_and_string_message_from_message_if_possible(passed_message)

        # Check result:
        received_message = received_message.replace("'", '"')
        expected_message = str(passed_message).replace("'", '"')
        expected_key = esgfpid.defaults.RABBIT_DEFAULT_ROUTING_KEY
        self.assertEquals(received_key, expected_key, 'Wrong routing key: %s' % received_key)
        self.assertEquals(expected_message, received_message, 'Wrong message.\nExpected: %s\nReceived: %s' % (expected_message, received_message))

    def test_get_message_and_routing_key_list(self):
        
        # Test variables:
        passed_message = [345]
        LOGGER.info('Message: %s' % passed_message)

        # Run code to be checked:
        received_key, received_message = rutils.get_routing_key_and_string_message_from_message_if_possible(passed_message)

        # Check result:
        received_message = received_message.replace("'", '"')
        expected_message = str(passed_message).replace("'", '"')
        expected_key = esgfpid.defaults.RABBIT_DEFAULT_ROUTING_KEY
        self.assertEquals(received_key, expected_key, 'Wrong routing key: %s' % received_key)
        self.assertEquals(expected_message, received_message, 'Wrong message.\nExpected: %s\nReceived: %s' % (expected_message, received_message))

    def test_get_message_and_routing_key_int(self):
        
        # Test variables:
        passed_message = 345
        LOGGER.info('Message: %s' % passed_message)

        # Run code to be checked:
        received_key, received_message = rutils.get_routing_key_and_string_message_from_message_if_possible(passed_message)

        # Check result:
        received_message = received_message.replace("'", '"')
        expected_message = str(passed_message).replace("'", '"')
        expected_key = esgfpid.defaults.RABBIT_DEFAULT_ROUTING_KEY
        self.assertEquals(received_key, expected_key, 'Wrong routing key: %s' % received_key)
        self.assertEquals(expected_message, received_message, 'Wrong message.\nExpected: %s\nReceived: %s' % (expected_message, received_message))

    def test_get_message_and_routing_key_bogus(self):
        
        # Test variables:
        def foo(): print('yeah')
        passed_message = foo
        LOGGER.info('Message: %s' % passed_message)

        # Run code to be checked:
        with self.assertRaises(ValueError):
            received_key, received_message = rutils.get_routing_key_and_string_message_from_message_if_possible(passed_message)


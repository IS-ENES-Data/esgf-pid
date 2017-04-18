import sys
import unittest
import mock
import logging
import copy
import esgfpid.defaults
import esgfpid.utils

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

LOGGER_TO_PASS = logging.getLogger('utils')
LOGGER_TO_PASS.addHandler(logging.NullHandler())


class ErrorMessageUtilsTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    #
    # Test getting routing key and string message
    #

    def test_get_message_and_routing_key_string_ok1(self):
        
        # Test variables:
        passed_messages = ['foo', 'bar', 'lorem ipsum lorem ipsum loooorem']

        # Run code to be checked:
        received_message = esgfpid.utils.format_error_message(passed_messages)

        # Check result:
        expected_message = (
            '****************************************\n'+
            '*** foo                              ***\n'+
            '*** bar                              ***\n'+
            '*** lorem ipsum lorem ipsum loooorem ***\n'+
            '****************************************')
        self.assertEquals(expected_message, received_message, 'Wrong message.\nExpected: %s\nReceived: %s' % (expected_message, received_message))


    def test_get_message_and_routing_key_string_ok2(self):
        
        # Test variables:
        passed_messages = ['foo']

        # Run code to be checked:
        received_message = esgfpid.utils.format_error_message(passed_messages)

        # Check result:
        expected_message = (
            '***********\n'+
            '*** foo ***\n'+
            '***********')
        self.assertEquals(expected_message, received_message, 'Wrong message.\nExpected: %s\nReceived: %s' % (expected_message, received_message))


import sys
import unittest
import mock
import logging
import copy
import os
import esgfpid.defaults
import esgfpid.rabbit.rabbitutils as rut

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

LOGGER_TO_PASS = logging.getLogger('utils')
LOGGER_TO_PASS.addHandler(logging.NullHandler())


class RabbitUtilsTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ########## (%s)' % os.path.basename(__file__))

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
        received_key, received_message = rut.get_routing_key_and_string_message_from_message_if_possible(passed_message)

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
        received_key, received_message = rut.get_routing_key_and_string_message_from_message_if_possible(passed_message)

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
            received_key, received_message = rut.get_routing_key_and_string_message_from_message_if_possible(passed_message)

    def test_get_message_and_routing_key_characters(self):
        
        # Test variables:
        passed_message = 'foo'
        LOGGER.info('Message: %s' % passed_message)

        # Run code to be checked:
        received_key, received_message = rut.get_routing_key_and_string_message_from_message_if_possible(passed_message)

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
        received_key, received_message = rut.get_routing_key_and_string_message_from_message_if_possible(passed_message)

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
        received_key, received_message = rut.get_routing_key_and_string_message_from_message_if_possible(passed_message)

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
        received_key, received_message = rut.get_routing_key_and_string_message_from_message_if_possible(passed_message)

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
        received_key, received_message = rut.get_routing_key_and_string_message_from_message_if_possible(passed_message)

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
            received_key, received_message = rut.get_routing_key_and_string_message_from_message_if_possible(passed_message)



    #
    # Test method to ensure URLs are a list
    #

    def test_ensure_urls_are_a_list_already_ok(self):

        # Test variables:
        args = dict(
            url_preferred = 'fave',
            urls_fallback = ['foo', 'bar']
        )

        # Run code to be tested:
        rut.ensure_urls_are_a_list(args, LOGGER_TO_PASS)

        # Check result:
        is_list = type(args['urls_fallback']) == type([1,2])
        self.assertTrue(is_list)

    def test_ensure_urls_are_a_list_not_yet(self):

        # Test variables:
        args = dict(
            url_preferred = 'fave',
            urls_fallback = 'foo'
        )

        # Run code to be tested:
        rut.ensure_urls_are_a_list(args, LOGGER_TO_PASS)

        # Check result:
        is_list = type(args['urls_fallback']) == type([1,2])
        self.assertTrue(is_list)

    def test_ensure_urls_are_a_list_is_tuple(self):

        # Test variables:
        args = dict(
            url_preferred = 'fave',
            urls_fallback = ('foo','bar')
        )

        # Run code to be tested:
        rut.ensure_urls_are_a_list(args, LOGGER_TO_PASS)

        # Check result:
        is_list = type(args['urls_fallback']) == type((1,2))
        self.assertTrue(is_list)

    def test_set_preferred_url_no_pref_one_other(self):

        # Test variables:
        args = dict(
            url_preferred = None,
            urls_fallback = ['foo'] # has to be list (or run "ensure_urls_are_a_list" before)
        )

        # Run code to be tested:
        rut.set_preferred_url(args, LOGGER_TO_PASS)

        # Check result:
        self.assertTrue(args['url_preferred'] == 'foo')
        self.assertTrue(args['urls_fallback'] == [])

    def test_ensure_urls_are_a_list_none(self):

        # Test variables:
        args = dict(
            url_preferred = 'fave',
            urls_fallback = None
        )

        # Run code to be tested:
        rut.ensure_urls_are_a_list(args, LOGGER_TO_PASS)

        # Check result:
        is_list = type(args['urls_fallback']) == type([1,2])
        self.assertTrue(is_list)

    def test_ensure_urls_are_a_list_bogus(self):

        # Test variables:
        def foo(): print('yeah')
        args = dict(
            url_preferred = 'fave',
            urls_fallback = foo
        )

        # Run code to be tested:
        with self.assertRaises(ValueError):
            rut.ensure_urls_are_a_list(args, LOGGER_TO_PASS)

    def test_set_preferred_url_no_pref_three_others(self):
        '''
        We test the random selection of the preferred URL.
        Three URLs are given, we select 100 times, each has
        to be selected at least 25 times for the test to pass.
        '''

        # Test variables:
        args = dict(
            url_preferred = None,
            urls_fallback = ['foo','bar','baz']
        )

        # Run code to be tested:
        def tryonce(args, result, LOGGER_TO_PASS):
            args_copy = copy.deepcopy(args)
            rut.set_preferred_url(args_copy, LOGGER_TO_PASS)
            which = args_copy['url_preferred']
            result[which] = result[which]+1

        result = dict(foo=0, bar=0, baz=0)
        n = 100
        for i in xrange(n):
            tryonce(args, result, LOGGER_TO_PASS)

        # Check result:
        self.assertTrue(result['foo'] >= 25, 'Foo was selected too few times: %i/%i (%s). Try again.' % (result['foo'], n, result))
        self.assertTrue(result['bar'] >= 25, 'Bar was selected too few times: %i/%i (%s). Try again. ' % (result['bar'], n, result))
        self.assertTrue(result['baz'] >= 25, 'Baz was selected too few times: %i/%i (%s). Try again.' % (result['baz'], n, result))

    #
    # Test method no_duplicates
    #

    def test_no_duplicates_one_ok(self):

        # Test variables:
        args = dict(
            url_preferred = 'foo',
            urls_fallback = ['foo', 'bar', 'baz'] # has to be list (or run "ensure_urls_are_a_list" before)
        )

        # Run code to be tested:
        rut.ensure_no_duplicate_urls(args, LOGGER_TO_PASS)

        # Check result:
        self.assertTrue(args['url_preferred'] == 'foo')
        self.assertTrue(len(args['urls_fallback']) == 2, 'Wrong number of fallback URLs: %s' % args['urls_fallback'])
        self.assertTrue(set(args['urls_fallback']) == set(['bar','baz']),  'Wrong fallback URLs: %s' % args['urls_fallback'])

    def test_no_duplicates_two_ok(self):

        # Test variables:
        args = dict(
            url_preferred = 'foo',
            urls_fallback = ['foo', 'foo', 'bar', 'baz'] # has to be list (or run "ensure_urls_are_a_list" before)
        )

        # Run code to be tested:
        rut.ensure_no_duplicate_urls(args, LOGGER_TO_PASS)

        # Check result:
        self.assertTrue(args['url_preferred'] == 'foo')
        self.assertTrue(len(args['urls_fallback']) == 2, 'Wrong number of fallback URLs: %s' % args['urls_fallback'])
        self.assertTrue(set(args['urls_fallback']) == set(['bar','baz']),  'Wrong fallback URLs: %s' % args['urls_fallback'])


    def test_no_duplicates_none_ok(self):

        # Test variables:
        args = dict(
            url_preferred = 'foo',
            urls_fallback = ['baz', 'bar'] # has to be list (or run "ensure_urls_are_a_list" before)
        )

        # Run code to be tested:
        rut.ensure_no_duplicate_urls(args, LOGGER_TO_PASS)

        # Check result:
        self.assertTrue(args['url_preferred'] == 'foo')
        self.assertTrue(len(args['urls_fallback']) == 2, 'Wrong number of fallback URLs: %s' % args['urls_fallback'])
        self.assertTrue(set(args['urls_fallback']) == set(['bar','baz']),  'Wrong fallback URLs: %s' % args['urls_fallback'])

    def test_no_duplicates_several_ok(self):

        # Test variables:
        args = dict(
            url_preferred = 'foo',
            urls_fallback = ['baz', 'bar', 'bar', 'baz'] # has to be list (or run "ensure_urls_are_a_list" before)
        )

        # Run code to be tested:
        rut.ensure_no_duplicate_urls(args, LOGGER_TO_PASS)

        # Check result:
        self.assertTrue(args['url_preferred'] == 'foo')
        self.assertTrue(len(args['urls_fallback']) == 2, 'Wrong number of fallback URLs: %s' % args['urls_fallback'])
        self.assertTrue(set(args['urls_fallback']) == set(['bar','baz']),  'Wrong fallback URLs: %s' % args['urls_fallback'])


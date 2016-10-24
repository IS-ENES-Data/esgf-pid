import unittest
import mock
import logging
import json
import requests
import os
import esgfpid.rabbit.asynchronous
import tests.mocks.responsemock
import mock

# Logging:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class RabbitAsynConnectorTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ########## (%s)' % os.path.basename(__file__))

    def tearDown(self):
        LOGGER.info('#############################')

    def make_testrabbit(self):
        cred = esgfpid.rabbit.connparams.get_credentials('userfoo', 'passwordfoo')
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(
            exchange_name = 'foo_exchange',
            url_preferred = 'first.host.com',
            urls_fallback = ['foo.com', 'bar.de'],
            credentials = cred
        )
        return testrabbit

    # Actual tests:

    def test_init_ok(self):

        # Test variables:
        cred = esgfpid.rabbit.connparams.get_credentials('userfoo', 'passwordfoo')

        # Run code to be tested:
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(
            exchange_name = 'foo_exchange',
            url_preferred = 'first.host.com',
            urls_fallback = ['foo.com', 'bar.de'],
            credentials = cred
        )

        # Check result:
        self.assertIsInstance(testrabbit, esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector, 'Constructor fail.')
        self.assertTrue(testrabbit._AsynchronousRabbitConnector__not_started_yet)


    #
    # Sending messages
    #

    def test_send_message_not_started_yet(self):
        testrabbit = self.make_testrabbit()

        self.assertTrue(testrabbit._AsynchronousRabbitConnector__not_started_yet)

        with self.assertRaises(ValueError):
            testrabbit.send_message_to_queue('message-foo')

        with self.assertRaises(ValueError):
            testrabbit.send_many_messages_to_queue(['a','b','c'])

    @mock.patch('esgfpid.rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector._AsynchronousRabbitConnector__create_thread')
    def test_send_message_ok(self, createpatch):

        # Prepare patch
        threadpatch = mock.MagicMock()
        createpatch.return_value = threadpatch

        # Preparations
        testrabbit = self.make_testrabbit()

        # Run code to be tested:
        testrabbit.start_rabbit_thread()
        testrabbit.send_message_to_queue('foo')
        testrabbit.send_many_messages_to_queue(['a','b','c'])

        # Check result
        threadpatch.start.assert_called_with()
        threadpatch.send_a_message.assert_called_with('foo')
        threadpatch.send_many_messages.assert_called_with(['a','b','c'])

    #
    # Gently finish
    #

    @mock.patch('esgfpid.rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector._AsynchronousRabbitConnector__create_thread')
    def test_gently_finish_ok(self, createpatch):

        # Prepare patch
        threadpatch = mock.MagicMock()
        createpatch.return_value = threadpatch
        threadpatch.is_alive.return_value = False

        # Preparations
        testrabbit = self.make_testrabbit()
        testrabbit.start_rabbit_thread()

        # Run code to be tested:
        testrabbit.finish_rabbit_thread()
        is_done = testrabbit.is_finished()
        any_left = testrabbit.any_leftovers()

        # Check result
        threadpatch.finish_gently.assert_called_with()
        self.assertTrue(is_done)
        self.assertFalse(any_left)

    @mock.patch('esgfpid.rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector._AsynchronousRabbitConnector__create_thread')
    def test_finish_reconnect_send_ok(self, createpatch):

        # Prepare patch
        threadpatch = mock.MagicMock()
        createpatch.return_value = threadpatch
        threadpatch.is_alive.return_value = False

        # Preparations
        testrabbit = self.make_testrabbit()
        testrabbit.start_rabbit_thread()

        # Run code to be tested:
        testrabbit.finish_rabbit_thread()
        testrabbit.reconnect()
        testrabbit.send_message_to_queue('foo')
        testrabbit.send_many_messages_to_queue(['a','b','c'])

        # Check result
        threadpatch.finish_gently.assert_called_with()
        threadpatch.send_a_message.assert_called_with('foo')
        threadpatch.send_many_messages.assert_called_with(['a','b','c'])

    @mock.patch('esgfpid.rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector._AsynchronousRabbitConnector__create_thread')
    def test_rescue_get_ok(self, createpatch):
        '''
        We finish the thread.
        Let's see if the leftovers (that we add articifially) can be retrieved!
        '''

        # Prepare patch
        threadpatch = mock.MagicMock()
        createpatch.return_value = threadpatch
        threadpatch.is_alive.return_value = False
        threadpatch.get_unpublished_messages_as_list.return_value = ['a','b','c']
        threadpatch.get_unconfirmed_messages_as_list.return_value = ['d','e','f']
        threadpatch.get_nacked_messages_as_list.return_value = ['g','h','i']

        # Preparations
        testrabbit = self.make_testrabbit()
        testrabbit.start_rabbit_thread()

        # Run code to be tested:
        testrabbit.finish_rabbit_thread()
        leftovers = testrabbit.get_leftovers()

        # Check result:
        expected_leftovers = ['a','b','c','d','e','f','g','h','i']
        self.assertEquals(leftovers, expected_leftovers, 'Leftovers: %s' % leftovers)


    @mock.patch('esgfpid.rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector._AsynchronousRabbitConnector__create_thread')
    def test_rescue_republish_ok(self, createpatch):
        '''
        We finish and reconnect.
        Let's see if the leftovers (that we add articifially) are published upon reconnect!
        '''

        # Prepare patch
        threadpatch = mock.MagicMock()
        createpatch.return_value = threadpatch
        threadpatch.is_alive.return_value = False
        threadpatch.get_unpublished_messages_as_list.return_value = ['a','b','c']
        threadpatch.get_unconfirmed_messages_as_list.return_value = ['d','e','f']
        threadpatch.get_nacked_messages_as_list.return_value = ['g','h','i']

        # Preparations
        testrabbit = self.make_testrabbit()
        testrabbit.start_rabbit_thread()

        # Run code to be tested:
        testrabbit.finish_rabbit_thread()
        any_left = testrabbit.any_leftovers()
        testrabbit.reconnect()

        # Check result:
        self.assertTrue(any_left)
        threadpatch.send_many_messages.assert_any_call(['g','h','i'])
        threadpatch.send_many_messages.assert_any_call(['d','e','f'])
        threadpatch.send_many_messages.assert_any_call(['a','b','c'])


    if False: # This test takes time!!
        @mock.patch('esgfpid.rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector._AsynchronousRabbitConnector__create_thread')
        def test_join_fails(self, createpatch):
            '''
            We finish, but the join fails.
            This test takes time!
            Rescueing messages should not work, so getting the leftovers should return an empty list.
            '''

            # Prepare patch
            threadpatch = mock.MagicMock()
            createpatch.return_value = threadpatch
            threadpatch.is_alive.return_value = True
            threadpatch.get_unpublished_messages_as_list.return_value = ['a','b','c']
            threadpatch.get_unconfirmed_messages_as_list.return_value = ['d','e','f']
            threadpatch.get_nacked_messages_as_list.return_value = ['g','h','i']

            # Preparations
            testrabbit = self.make_testrabbit()
            testrabbit.start_rabbit_thread()

            # Run code to be tested:
            testrabbit.finish_rabbit_thread()
            leftovers = testrabbit.get_leftovers()

            # Check result:
            self.assertEquals(leftovers, [], 'Leftovers: %s' % leftovers)

    @mock.patch('esgfpid.rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector._AsynchronousRabbitConnector__create_thread')
    def test_force_finish_ok(self, createpatch):
        '''
        We force finish.
        Let's see if the leftovers (that we add articifially) can be retrieved!
        '''

        # Prepare patch
        threadpatch = mock.MagicMock()
        createpatch.return_value = threadpatch
        threadpatch.is_alive.return_value = False
        threadpatch.get_unpublished_messages_as_list.return_value = ['a','b','c']
        threadpatch.get_unconfirmed_messages_as_list.return_value = ['d','e','f']
        threadpatch.get_nacked_messages_as_list.return_value = ['g','h','i']

        # Preparations
        testrabbit = self.make_testrabbit()
        testrabbit.start_rabbit_thread()

        # Run code to be tested:
        testrabbit.force_finish_rabbit_thread()
        leftovers = testrabbit.get_leftovers()

        # Check result:
        expected_leftovers = ['a','b','c','d','e','f','g','h','i']
        self.assertEquals(leftovers, expected_leftovers, 'Leftovers: %s' % leftovers)
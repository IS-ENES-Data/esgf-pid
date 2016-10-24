import unittest
import mock
import logging
import json
import requests
import esgfpid.rabbit.asynchronous
import tests.mocks.responsemock
import tests.mocks.pikamock
import mock
import time
import os

# Logging:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

WAIT = 1

class RabbitAsynModuleTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ########## (%s)' % os.path.basename(__file__))

    def tearDown(self):
        LOGGER.info('#############################')

    def make_testrabbit(self):
        cred = esgfpid.rabbit.connparams.get_credentials('userfoo', 'passwordfoo')
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(
            exchange_name = 'foo_exchange',
            url_preferred = 'first',
            urls_fallback = ['foo', 'bar'],
            credentials = cred
        )
        return testrabbit

    def make_connection_and_channel_mock(self):
        conn = tests.mocks.pikamock.MockPikaSelectConnection()
        return conn, conn.channel()


    # Actual tests:


    def test_init_ok(self):

        # Test variables:
        cred = esgfpid.rabbit.connparams.get_credentials('userfoo', 'passwordfoo')

        # Run code to be tested:
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(
            exchange_name = 'foo_exchange',
            url_preferred = 'first',
            urls_fallback = ['foo', 'bar'],
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


    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_send_message_ok(self, pleasepatch):
        '''
        Does sending messages via the feeder work?
        "on_delivery_confirmation" is not mocked here, so confirmation is not checked!
        '''
        LOGGER.info('test_send_message_ok')
        # Preparations
        testrabbit = self.make_testrabbit()

        # Prepare patch
        connection,channel = self.make_connection_and_channel_mock()
        #print('CONN: %s' % connection)
        #print('CHAN: %s' % channel)
        pleasepatch.return_value = connection

        # After starting thread, we also have to call the on_channel_open, which is usually called by RabbitMQ:
        testrabbit.start_rabbit_thread()
        time.sleep(WAIT)
        testrabbit.rabbit_thread.builder.on_channel_open(channel)
     
        # Run code to be tested:
        testrabbit.send_message_to_queue('foo')
        testrabbit.send_many_messages_to_queue(['a','b','c'])

        # Check result
        self.assertEquals(channel.publish_counter, 4)

    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_finish_ok(self, pleasepatch):
        '''
        Does sending messages via the feeder work?
        "on_delivery_confirmation" is not mocked here, so confirmation is not checked!
        '''

        LOGGER.info("test_finish_ok")

        # Preparations
        testrabbit = self.make_testrabbit()

        # Prepare patch
        connection,channel = self.make_connection_and_channel_mock()
        pleasepatch.return_value = connection

        # After starting thread, we also have to call the on_channel_open, which is usually called by RabbitMQ:
        testrabbit.start_rabbit_thread()
        time.sleep(WAIT)
        testrabbit.rabbit_thread.builder.on_channel_open(channel)
     
        # Run code to be tested:
        testrabbit.finish_rabbit_thread()

        # Check result
        self.assertTrue(testrabbit.rabbit_thread.statemachine.is_permanently_unavailable())
        self.assertTrue(testrabbit.rabbit_thread.statemachine.closed_by_publisher)
        self.assertTrue(testrabbit.rabbit_thread._connection.is_closed)
        self.assertFalse(testrabbit.rabbit_thread._connection.is_open)

    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_force_finish_ok(self, pleasepatch):
        '''
        Does sending messages via the feeder work?
        "on_delivery_confirmation" is not mocked here, so confirmation is not checked!
        '''
        LOGGER.info("test_force_finish_ok")

        # Preparations
        testrabbit = self.make_testrabbit()

        # Prepare patch
        connection,channel = self.make_connection_and_channel_mock()
        pleasepatch.return_value = connection

        # After starting thread, we also have to call the on_channel_open, which is usually called by RabbitMQ:
        testrabbit.start_rabbit_thread()
        time.sleep(WAIT)
        testrabbit.rabbit_thread.builder.on_channel_open(channel)
     
        # Run code to be tested:
        testrabbit.force_finish_rabbit_thread()

        # Check result
        self.assertTrue(testrabbit.rabbit_thread.statemachine.is_permanently_unavailable())
        self.assertTrue(testrabbit.rabbit_thread.statemachine.closed_by_publisher)
        self.assertFalse(connection.is_open)

    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_operation_not_allowed_ok(self, pleasepatch):
        '''
        Does sending messages via the feeder work?
        "on_delivery_confirmation" is not mocked here, so confirmation is not checked!
        '''

        # Preparations
        testrabbit = self.make_testrabbit()

        # Prepare patch
        connection,channel = self.make_connection_and_channel_mock()
        pleasepatch.return_value = connection

        # After starting thread, we also have to call the on_channel_open, which is usually called by RabbitMQ:
        testrabbit.start_rabbit_thread()
        time.sleep(WAIT)
        testrabbit.rabbit_thread.builder.on_channel_open(channel)

        # Have to set thread manually to alive, as we cannot mock this (cannot mock ioloop...)
        testrabbit.rabbit_thread.is_alive = mock.MagicMock(return_value=True)
     
        # Run code to be tested:
        with self.assertRaises(esgfpid.rabbit.asynchronous.exceptions.OperationNotAllowed):
            testrabbit.rabbit_thread.get_unpublished_messages_as_list()
        with self.assertRaises(esgfpid.rabbit.asynchronous.exceptions.OperationNotAllowed):
            testrabbit.rabbit_thread.get_unconfirmed_messages_as_list()
        with self.assertRaises(esgfpid.rabbit.asynchronous.exceptions.OperationNotAllowed):
            testrabbit.rabbit_thread.get_nacked_messages_as_list()

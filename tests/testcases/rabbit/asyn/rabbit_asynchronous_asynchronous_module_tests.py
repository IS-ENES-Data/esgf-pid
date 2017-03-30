import unittest
import mock
import logging
import json
import requests
import esgfpid.rabbit.asynchronous
from esgfpid.rabbit.asynchronous.exceptions import OperationNotAllowed
import tests.mocks.responsemock
import tests.mocks.pikamock
import mock
import time
import os

# Logging:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

import globalvar
if globalvar.QUICK_ONLY:
    print('Skipping slow tests in module "%s".' % __name__)

class RabbitAsynModuleTestCase(unittest.TestCase):

    slow_message = '\nRunning a slow test (avoid by using -ls flag).'

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)
        self.testrabbit = None
        self.thread = None

    def tearDown(self):
        LOGGER.info('#############################')

    def make_nodemanager(self):
        # Make a node manager with three nodes:
        node_manager = esgfpid.rabbit.nodemanager.NodeManager()
        node_manager.add_trusted_node(
            username='userfoo',
            password='passwordfoo',
            host='first',
            exchange_name='foo_exchange'
        )
        node_manager.add_open_node(
            username='userfoo',
            password='passwordfoo',
            host='foo',
            exchange_name='foo_exchange'
        )
        node_manager.add_open_node(
            username='userfoo',
            password='passwordfoo',
            host='bar',
            exchange_name='foo_exchange'
        )
        return node_manager

    def make_testrabbit(self):

        node_manager = self.make_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(node_manager)
        conn = tests.mocks.pikamock.MockPikaSelectConnection()

        self.rabbit = testrabbit
        self.thread = self.rabbit._AsynchronousRabbitConnector__thread
        self.builder = self.thread._RabbitThread__builder
        self.connection = conn
        callback = None
        self.channel = conn.channel(callback)


    # Actual tests:


    '''
    Test the constructor.
    '''
    def test_init_ok(self):

        # Test variables:
        node_manager = esgfpid.rabbit.nodemanager.NodeManager()

        # Run code to be tested:
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(node_manager)

        # Check result:
        self.assertIsInstance(testrabbit, esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector, 'Constructor fail.')
        self.assertTrue(testrabbit._AsynchronousRabbitConnector__not_started_yet)


    #
    # Sending messages
    #

    '''
    Test if we can send messages before starting the thread.
    We shouldn't!
    '''
    def test_send_message_not_started_yet(self):
        self.make_testrabbit()

        self.assertTrue(self.rabbit._AsynchronousRabbitConnector__not_started_yet)

        with self.assertRaises(OperationNotAllowed):
            self.rabbit.send_message_to_queue('message-foo')

        with self.assertRaises(OperationNotAllowed):
                self.rabbit.send_many_messages_to_queue(['a','b','c'])

    @unittest.skipIf(globalvar.QUICK_ONLY, '(this test is slow)')
    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_send_message_ok_no_confirm(self, open_connection_patch):

        print('\nThis test may take some seconds, as it waits for confirms that can not come...')
        print(self.slow_message)

        # Patch the method that tries to open a conneciton to RabbitMQ:
        open_connection_patch = mock.MagicMock()

        # Make a test object:
        self.make_testrabbit()

        # Start the thread. This would normally trigger
        # the connection, but we patched that part, and
        # have to  it manually:
        self.rabbit.start_rabbit_thread()
        # This would usually be done by Rabbit via callback,
        # when start_rabbit_thread is called:
        self.thread._connection = self.connection
        self.builder.on_connection_open(None)
        self.builder.on_channel_open(self.channel)

        # Now run the code to be tested and close the rabbit:
        self.rabbit.send_message_to_queue({"bla":"bla"})
        self.rabbit.send_many_messages_to_queue([{"bla":"bla"},{"bla":"bla"},{"bla":"bla"}])
        self.rabbit.finish_rabbit_thread()

        # Check result: Are 4 messages published?
        self.assertEquals(self.channel.publish_counter, 4)

    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_finish_ok(self, open_connection_patch):

        # Patch the method that tries to open a conneciton to RabbitMQ:
        open_connection_patch = mock.MagicMock()

        # Make a test object:
        self.make_testrabbit()

        # Start the thread. This would normally trigger
        # the connection, but we patched that part, and
        # have to  it manually:
        self.rabbit.start_rabbit_thread()
        # This would usually be done by Rabbit via callback,
        # when start_rabbit_thread is called:
        self.thread._connection = self.connection
        self.builder.on_connection_open(None)
        self.builder.on_channel_open(self.channel)

        # Now run the code to be tested and close the rabbit:
        self.rabbit.finish_rabbit_thread()

        # Check result
        self.assertTrue(self.builder.statemachine.is_PERMANENTLY_UNAVAILABLE())
        self.assertTrue(self.builder.statemachine.get_detail_closed_by_publisher())
        self.assertTrue(self.thread._connection.is_closed)
        self.assertFalse(self.thread._connection.is_open)

    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_force_finish_ok(self, open_connection_patch):

        # Patch the method that tries to open a conneciton to RabbitMQ:
        open_connection_patch = mock.MagicMock()

        # Make a test object:
        self.make_testrabbit()

        # Start the thread. This would normally trigger
        # the connection, but we patched that part, and
        # have to  it manually:
        self.rabbit.start_rabbit_thread()
        # This would usually be done by Rabbit via callback,
        # when start_rabbit_thread is called:
        self.thread._connection = self.connection
        self.builder.on_connection_open(None)
        self.builder.on_channel_open(self.channel)

        # Now run the code to be tested and close the rabbit:
        self.rabbit.force_finish_rabbit_thread()

        # Check result
        self.assertTrue(self.builder.statemachine.is_PERMANENTLY_UNAVAILABLE())
        self.assertTrue(self.builder.statemachine.get_detail_closed_by_publisher())
        self.assertTrue(self.thread._connection.is_closed)
        self.assertFalse(self.thread._connection.is_open)

import unittest
import mock
import logging
import esgfpid.rabbit.asynchronous
from esgfpid.rabbit.asynchronous.exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

class RabbitAsynConnectorTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    #
    # Init
    #

    def test_init_ok(self):

        # Test variables:
        nodemanager = TESTHELPERS.get_nodemanager()

        # Run code to be tested:
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)

        # Check result:
        self.assertIsInstance(testrabbit, esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector, 'Constructor fail.')
        self.assertTrue(testrabbit._AsynchronousRabbitConnector__not_started_yet)

    #
    # Sending messages
    #

    def test_send_message_not_started_yet(self):

        # Preparation:
        testrabbit = TESTHELPERS.get_asynchronous_rabbit()
        self.assertTrue(testrabbit._AsynchronousRabbitConnector__not_started_yet)

        # Run code to be tested:
        with self.assertRaises(OperationNotAllowed):
            testrabbit.send_message_to_queue('message-foo')
        with self.assertRaises(OperationNotAllowed):
            testrabbit.send_many_messages_to_queue(['a','b','c'])

    @mock.patch('esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector._AsynchronousRabbitConnector__create_thread')
    def test_send_message_ok(self, createpatch):

        # Prepare patch
        threadpatch = TESTHELPERS.get_thread_mock()
        createpatch.return_value = threadpatch

        # Preparations
        testrabbit = TESTHELPERS.get_asynchronous_rabbit()

        # Run code to be tested:
        testrabbit.start_rabbit_thread()
        testrabbit._AsynchronousRabbitConnector__statemachine.set_to_available()
        testrabbit.send_message_to_queue('foo')
        testrabbit.send_many_messages_to_queue(['a','b','c'])

        # Check result
        self.assertTrue(threadpatch.start_was_called)
        self.assertTrue(threadpatch.num_message_events>4)
        queue = testrabbit._AsynchronousRabbitConnector__unpublished_messages_queue
        queue_content = []
        while not queue.empty():
            queue_content.append(queue.get())
        self.assertIn('foo', queue_content)
        self.assertIn('a', queue_content)
        self.assertIn('b', queue_content)
        self.assertIn('c', queue_content)

    #
    # Gently finish
    #

    @mock.patch('esgfpid.rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector._AsynchronousRabbitConnector__create_thread')
    def test_gently_finish_ok(self, createpatch):

        # Prepare patch
        threadpatch = TESTHELPERS.get_thread_mock()
        createpatch.return_value = threadpatch

        # Preparations
        testrabbit = TESTHELPERS.get_asynchronous_rabbit()
        testrabbit.start_rabbit_thread()
        testrabbit._AsynchronousRabbitConnector__statemachine.set_to_available()

        # Run code to be tested:
        testrabbit.finish_rabbit_thread()

        # Check result
        self.assertTrue(threadpatch.was_gently_finished)
        self.assertTrue(threadpatch.join_was_called)
        self.assertFalse(threadpatch.is_alive())
        self.assertTrue(testrabbit.is_finished())

    #
    # Force finish
    #

    @mock.patch('esgfpid.rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector._AsynchronousRabbitConnector__create_thread')
    def test_force_finish_ok(self, createpatch):
        '''
        We force finish.
        Let's see if the leftovers (that we add articifially) can be retrieved!
        '''

        # Prepare patch
        threadpatch = TESTHELPERS.get_thread_mock()
        createpatch.return_value = threadpatch

        # Preparations
        testrabbit = TESTHELPERS.get_asynchronous_rabbit()
        testrabbit.start_rabbit_thread()
        testrabbit._AsynchronousRabbitConnector__statemachine.set_to_available()

        # Run code to be tested:
        testrabbit.force_finish_rabbit_thread()

        # Check result:
        self.assertTrue(threadpatch.was_force_finished)
        self.assertTrue(threadpatch.join_was_called)
        self.assertFalse(threadpatch.is_alive())
        self.assertTrue(testrabbit.is_finished())

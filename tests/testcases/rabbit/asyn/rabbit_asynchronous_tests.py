import unittest
import mock
import logging
import datetime
import time
import esgfpid.rabbit.asynchronous
from esgfpid.rabbit.asynchronous.exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

import globalvar
if globalvar.QUICK_ONLY:
    print('Skipping slow tests in module "%s".' % __name__)


class RabbitAsynConnectorTestCase(unittest.TestCase):

    slow_message = '\nRunning a slow test (avoid by using -ls flag).'


    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def assert_messages_are_in_queue(self, queue, list_of_messages):
        queue_content = []
        while not queue.empty():
            queue_content.append(queue.get(False))
        for msg in list_of_messages:
            self.assertIn(msg, queue_content)

    #
    # Init
    #

    '''
    Test whether instances of rabbitconnector and thread
    are created.
    '''
    def test_init_ok(self):

        # Test variables:
        nodemanager = TESTHELPERS.get_nodemanager()

        # Run code to be tested:
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)

        # Check result:
        self.assertIsInstance(testrabbit, esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector, 'Constructor fail.')
        self.assertTrue(testrabbit._AsynchronousRabbitConnector__not_started_yet)
        thread = testrabbit._AsynchronousRabbitConnector__thread
        self.assertIsInstance(thread, esgfpid.rabbit.asynchronous.rabbitthread.RabbitThread, 'Constructor fail.')


    #
    # Start thread
    #

    '''
    Test whether the start method does the necessary things.
    We expect it to change the state, and to run the thread.
    '''
    @unittest.skipIf(globalvar.QUICK_ONLY, '(this test is slow)')
    def test_start_thread_ok(self):

        print(self.slow_message)

        # Preparation:
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        
        # Mock the builder (so it cannot start a connection):
        def side_effect_first_connection():
            LOGGER.debug('Pretending to do something in the thread.run()')
            time.sleep(0.5)
            LOGGER.debug('Finished pretending to do something in the thread.run()')
        buildermock = mock.MagicMock()
        buildermock.first_connection = mock.MagicMock()
        buildermock.first_connection.side_effect = side_effect_first_connection
        testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__builder = buildermock

        # Check preconditions:
        self.assertTrue(testrabbit._AsynchronousRabbitConnector__not_started_yet)
        self.assertTrue(testrabbit._AsynchronousRabbitConnector__statemachine.is_NOT_STARTED_YET())

        # Run code to be tested:
        # This runs the thread, which triggers building a connection.
        # In this test, it calls the side effect defined above, which blocks for
        # a second. Afterwards, the thread should be finished.
        testrabbit.start_rabbit_thread()

        # Check results:
        # Check if thread is alive:
        self.assertTrue(testrabbit._AsynchronousRabbitConnector__thread.is_alive())
        # Join the thread...
        print("Joining...")
        testrabbit._AsynchronousRabbitConnector__thread.join()
        print("Joining done...")
        # Check if the thread has ended:
        self.assertFalse(testrabbit._AsynchronousRabbitConnector__thread.is_alive())
        # Check state:
        self.assertFalse(testrabbit._AsynchronousRabbitConnector__not_started_yet)
        self.assertTrue(testrabbit._AsynchronousRabbitConnector__statemachine.is_WAITING_TO_BE_AVAILABLE())
        # Check if run was called:
        buildermock.first_connection.assert_called()


    #
    # Sending messages
    #

    '''
    Test behaviour when we try sending messages but the
    thread was not started yet.
    It should raise an exception.
    '''
    def test_send_message_not_started_yet(self):

        # Preparation:
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        self.assertTrue(testrabbit._AsynchronousRabbitConnector__not_started_yet)

        # Run code to be tested:
        with self.assertRaises(OperationNotAllowed):
            testrabbit.send_message_to_queue('message-foo')
        with self.assertRaises(OperationNotAllowed):
            testrabbit.send_many_messages_to_queue(['a','b','c'])

    '''
    Test behaviour when we try sending messages but the
    thread was not started yet.
    It should raise an exception.
    '''
    def test_send_message_not_started_yet_2(self):

        # Preparation:
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        testrabbit._AsynchronousRabbitConnector__not_started_yet = False
        self.assertFalse(testrabbit._AsynchronousRabbitConnector__not_started_yet)
        self.assertTrue(testrabbit._AsynchronousRabbitConnector__statemachine.is_NOT_STARTED_YET())

        # Run code to be tested:
        with self.assertRaises(OperationNotAllowed):
            testrabbit.send_message_to_queue('message-foo')
        with self.assertRaises(OperationNotAllowed):
            testrabbit.send_many_messages_to_queue(['a','b','c'])

    '''
    Test behaviour when we send messages when the thread
    was properly started.

    We expect the message to be put into the queue.
    We expect the publish event to be handed by the connection
    to the feeder module.
    '''
    def test_send_message_ok(self):

        # Preparations
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        testrabbit._AsynchronousRabbitConnector__statemachine.set_to_available()
        testrabbit._AsynchronousRabbitConnector__not_started_yet = False

        # Mock the connection (it has to hand the event over to the feeder mock):
        connectionmock = testrabbit._AsynchronousRabbitConnector__thread._connection = TESTHELPERS.get_connection_mock()

        # Mock the feeder (it has to receive the publish event):
        feedermock = testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__feeder = mock.MagicMock()

        # Run code to be tested:
        testrabbit.send_message_to_queue('foo')
        testrabbit.send_many_messages_to_queue(['a','b','c'])

        # Check that publish was called:
        feedermock.publish_message.assert_called()
        self.assertTrue(feedermock.publish_message.call_count>=4)

        # Check that the four messages were put into the queue:
        queue = testrabbit._AsynchronousRabbitConnector__unpublished_messages_queue
        self.assert_messages_are_in_queue(queue, ['foo', 'a', 'b', 'c'])


    def test_send_message_waiting(self):

        # Preparations
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        testrabbit._AsynchronousRabbitConnector__statemachine.set_to_waiting_to_be_available()
        testrabbit._AsynchronousRabbitConnector__not_started_yet = False

        # Mock the connection (it has to hand the event over to the feeder mock):
        connectionmock = testrabbit._AsynchronousRabbitConnector__thread._connection = TESTHELPERS.get_connection_mock()

        # Mock the feeder (it has to receive the publish event):
        feedermock = testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__feeder = mock.MagicMock()

        # Run code to be tested:
        testrabbit.send_many_messages_to_queue(['a','b','c'])
        testrabbit.send_message_to_queue('foo')

        # Check that publish was NOT called:
        feedermock.publish_message.assert_not_called()

        # Check that the four messages were put into the queue:
        queue = testrabbit._AsynchronousRabbitConnector__unpublished_messages_queue
        self.assert_messages_are_in_queue(queue, ['foo', 'a', 'b', 'c'])

    def test_send_message_unavail(self):

        # Preparations
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        testrabbit._AsynchronousRabbitConnector__statemachine.set_to_permanently_unavailable()
        testrabbit._AsynchronousRabbitConnector__not_started_yet = False

        # Mock the feeder (it has to receive the publish event):
        feedermock = testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__feeder = mock.MagicMock()

        # Mock the connection (it has to hand the event over to the feeder mock):
        connectionmock = testrabbit._AsynchronousRabbitConnector__thread._connection = TESTHELPERS.get_connection_mock()

        # Run code to be tested:
        testrabbit.send_message_to_queue('foo')
        testrabbit.send_many_messages_to_queue(['a','b','c'])

        # Check that publish was NOT called:
        feedermock.publish_message.assert_not_called()

        # Check that the four messages were NOT put into the queue:
        queue = testrabbit._AsynchronousRabbitConnector__unpublished_messages_queue
        with self.assertRaises(Queue.Empty):
            queue.get(False)

    def test_send_message_user_closed(self):

        # Preparations
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        testrabbit._AsynchronousRabbitConnector__statemachine.set_to_permanently_unavailable()
        testrabbit._AsynchronousRabbitConnector__statemachine.set_detail_closed_by_publisher()
        testrabbit._AsynchronousRabbitConnector__not_started_yet = False

        # Mock the feeder (it has to receive the publish event):
        feedermock = testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__feeder = mock.MagicMock()

        # Mock the connection (it has to hand the event over to the feeder mock):
        connectionmock = testrabbit._AsynchronousRabbitConnector__thread._connection = TESTHELPERS.get_connection_mock()

        # Run code to be tested:
        with self.assertRaises(OperationNotAllowed):
            testrabbit.send_message_to_queue('foo')
        with self.assertRaises(OperationNotAllowed):
            testrabbit.send_many_messages_to_queue(['a','b','c'])

        # Check that publish was NOT called:
        feedermock.publish_message.assert_not_called()

        # Check that the four messages were NOT put into the queue:
        queue = testrabbit._AsynchronousRabbitConnector__unpublished_messages_queue
        with self.assertRaises(Queue.Empty):
            queue.get(False)


    #
    # Gently finish
    #

    def test_gently_finish_ok(self):

        # Preparations
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        testrabbit._AsynchronousRabbitConnector__statemachine.set_to_available()

        # Mock the wait-event, otherwise the library blocks, because the
        # wait event would be unblocked in a patched function...
        testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__gently_finish_ready = wait_event_mock = mock.MagicMock()

        # Mock the join function, as it cannot join if the thread was not started.
        testrabbit._AsynchronousRabbitConnector__thread.join =  joinmock = mock.MagicMock()

        # Mock shutter (it has to receive the close event)
        testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__shutter = shuttermock = mock.MagicMock()

        # Mock the connection (it has to hand the event over to the feeder mock):
        connectionmock = testrabbit._AsynchronousRabbitConnector__thread._connection = TESTHELPERS.get_connection_mock()

        # Run code to be tested:
        testrabbit.finish_rabbit_thread()

        # Check result
        shuttermock.finish_gently.assert_called()
        wait_event_mock.wait.assert_called()
        joinmock.assert_called()

    #
    # Force finish
    #

    def test_force_finish_ok(self):

        # Preparations
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        testrabbit._AsynchronousRabbitConnector__statemachine.set_to_available()

        # Mock the join function, as it cannot join if the thread was not started.
        testrabbit._AsynchronousRabbitConnector__thread.join =  joinmock = mock.MagicMock()

        # Mock shutter (it has to receive the close event)
        testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__shutter = shuttermock = mock.MagicMock()
        
        # Mock the connection (it has to hand the event over to the feeder mock):
        connectionmock = testrabbit._AsynchronousRabbitConnector__thread._connection = TESTHELPERS.get_connection_mock()

        # Run code to be tested:
        testrabbit.force_finish_rabbit_thread()

        # Check result
        shuttermock.force_finish.assert_called()
        joinmock.assert_called()

    @unittest.skipIf(globalvar.QUICK_ONLY, '(this test is slow)')
    def test_force_finish_join_fails(self):

        print(self.slow_message)

        # Preparations
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        testrabbit._AsynchronousRabbitConnector__statemachine.set_to_available()

        # Mock the join function, as it cannot join if the thread was not started.
        testrabbit._AsynchronousRabbitConnector__thread.join =  joinmock = mock.MagicMock()
        testrabbit._AsynchronousRabbitConnector__thread.is_alive =  alivemock = mock.MagicMock()
        alivemock.side_effect = [True,False,False,False,False,False,False]

        # Mock shutter (it has to receive the close event)
        testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__shutter = shuttermock = mock.MagicMock()
        
        # Mock the connection (it has to hand the event over to the feeder mock):
        connectionmock = testrabbit._AsynchronousRabbitConnector__thread._connection = TESTHELPERS.get_connection_mock()

        # Run code to be tested:
        testrabbit.force_finish_rabbit_thread()

        # Check result
        shuttermock.force_finish.assert_called()
        joinmock.assert_called()


    #
    # Kleinkram
    #

    def test_is_finished(self):

        # Preparation:
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)

        # Mock thread.is_alive and test getter for it:
        thread = testrabbit._AsynchronousRabbitConnector__thread
        thread.is_alive = mock.MagicMock()
        thread.is_alive.return_value = True
        self.assertFalse(testrabbit.is_finished())
        thread.is_alive.return_value = False
        self.assertTrue(testrabbit.is_finished())

    def test_tell_stop_waiting(self):

        # Preparation:
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__gently_finish_ready = mock.MagicMock()

        # Run code to be tested:
        testrabbit._AsynchronousRabbitConnector__thread.tell_publisher_to_stop_waiting_for_gentle_finish()

        # Check result
        testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__gently_finish_ready.set.assert_called()

    def test_unblock_events(self):

        # Preparation:
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)

        # Run code to be tested:
        testrabbit._AsynchronousRabbitConnector__thread.unblock_events()

        # Can't really check results... Cannot block the events to unblock them here,
        # without opening a thread...

    def test_wait_for_connection(self):

        # Preparation:
        nodemanager = TESTHELPERS.get_nodemanager()
        testrabbit = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
        testrabbit._AsynchronousRabbitConnector__statemachine.set_to_available()
        testrabbit._AsynchronousRabbitConnector__not_started_yet = False

        # Mock the feeder (it has to receive the publish event):
        feedermock = testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__feeder = mock.MagicMock()

        # Mock event...
        # So when the synchronisation is done, a connection is there...
        # When the event is called, we mock the connection (it has to hand the event over to the feeder mock):

        def sync_side_effect():
            connectionmock = testrabbit._AsynchronousRabbitConnector__thread._connection = TESTHELPERS.get_connection_mock()
        sync_event_mock = mock.MagicMock()
        sync_event_mock.wait.side_effect = sync_side_effect
        testrabbit._AsynchronousRabbitConnector__thread._RabbitThread__connection_is_set =  sync_event_mock

        # Connection must be None first:
        testrabbit._AsynchronousRabbitConnector__thread._connection = None

        # Run code to be tested:
        testrabbit.send_message_to_queue('foo')

        # Check if the event was unblocked:
        sync_event_mock.wait.assert_called()

        # Check that publish was called:
        feedermock.publish_message.assert_called()

        # Check that the four messages were put into the queue:
        queue = testrabbit._AsynchronousRabbitConnector__unpublished_messages_queue
        self.assert_messages_are_in_queue(queue, ['foo'])
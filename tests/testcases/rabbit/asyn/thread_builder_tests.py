import unittest
import mock
import logging
import datetime
import pika
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

class ThreadBuilderTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def make_builder(self, args=None):

        statemachine = esgfpid.rabbit.asynchronous.thread_statemachine.StateMachine()
        confirmer = esgfpid.rabbit.asynchronous.thread_confirmer.Confirmer()
        nodemanager = TESTHELPERS.get_nodemanager()

        thread = mock.MagicMock()
        thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER=999
        thread.ERROR_TEXT_CONNECTION_FORCE_CLOSED='(forced finish)'
        thread.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN='(not reopen)'
        thread.ERROR_TEXT_CONNECTION_PERMANENT_ERROR='(permanent error)'
        returnhandler = mock.MagicMock()
        shutter = mock.MagicMock()

        builder = esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder(
            thread,
            statemachine,
            confirmer,
            returnhandler,
            shutter,
            nodemanager)

        return builder

    #
    # first connection
    #

    '''
    Test first connection, normal case.
    '''
    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_first_connection_ok(self, connpatch):

        # Patch
        def make_conn():
            builder.thread._connection = mock.MagicMock()
            builder.thread._connection.ioloop = mock.MagicMock()
            builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        connpatch.side_effect = make_conn

        # Preparation:
        builder = self.make_builder()

        # Run code to be tested:
        builder.first_connection()

        # Check result:
        self.assertEquals(connpatch.call_count, 1)
        builder.thread._connection.ioloop.start.assert_called_with()
        builder.thread.continue_gently_closing_if_applicable.assert_called_with()

    '''
    Test first connection, ProbableAuthenticationError.
    The on_connection_error method is called by the library itself,
    and the ioloop is started anyway.
    '''
    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_first_connection_auth_error(self, connpatch):

        # Create a connection object
        mock_connection = mock.MagicMock()
        mock_connection.ioloop = mock.MagicMock()
        mock_connection.ioloop.start = mock.MagicMock()
        side_effect_first_call = pika.exceptions.ProbableAuthenticationError
        side_effect_second_call = None
        mock_connection.ioloop.start.side_effect = (side_effect_first_call, side_effect_second_call)

        # Patched method:
        # This is what should have happened during the patched method...
        def make_conn():
            builder.thread._connection = mock_connection
            builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        connpatch.side_effect = make_conn

        # Preparation:
        builder = self.make_builder()

        # Mock the error method
        builder.on_connection_error = mock.MagicMock()

        # Run code to be tested:
        builder.first_connection()

        # Check result:
        self.assertEquals(connpatch.call_count, 1)
        builder.on_connection_error.assert_called()
        mock_connection.ioloop.start.assert_called_with()
        builder.thread.continue_gently_closing_if_applicable.assert_called_with()

    '''
    Test first connection, any other exception.
    I am not sure this can actually happen, or how!
    The on_connection_error method is called by the library itself,
    and the ioloop is started anyway.
    '''
    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_first_connection_any_error(self, connpatch):

        # Create a connection object
        mock_connection = mock.MagicMock()
        mock_connection.ioloop = mock.MagicMock()
        mock_connection.ioloop.start = mock.MagicMock()
        side_effect_first_call = KeyError # any error...
        side_effect_second_call = None
        mock_connection.ioloop.start.side_effect = (side_effect_first_call, side_effect_second_call)

        # Patched method:
        # This is what should have happened during the patched method...
        def make_conn():
            builder.thread._connection = mock_connection
            builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        connpatch.side_effect = make_conn

        # Preparation:
        builder = self.make_builder()

        # Mock the error method
        builder.on_connection_error = mock.MagicMock()

        # Run code to be tested:
        builder.first_connection()

        # Check result:
        self.assertEquals(connpatch.call_count, 1)
        builder.on_connection_error.assert_called()
        mock_connection.ioloop.start.assert_called_with()
        builder.thread.continue_gently_closing_if_applicable.assert_called_with()

    #
    # on_connection_open
    #

    '''
    We test the actions when RabbitMQ has opened a connection.
    It triggers opening the channel and stops the publisher from
    blocking.
    '''
    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_rabbit_channel')
    def test_on_connection_open_ok(self, channelpatch):

        # Patch
        channelpatch.return_value = mock.MagicMock()

        # Preparation:
        builder = self.make_builder()

        # Run code to be tested:
        unused_connection = None
        builder.on_connection_open(unused_connection)

        # Check result:
        self.assertEquals(channelpatch.call_count, 1)
        builder.thread.tell_publisher_to_stop_waiting_for_thread_to_accept_events.assert_called_with()

    #
    # on_channel_open
    #

    '''
    We test the actions when RabbitMQ has opened a channel.
    The channel is set to confirm-deliveries and the callbacks are added.
    '''
    def test_on_channel_open_ok(self):

        # Preparation:
        builder = self.make_builder()
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        builder.thread.get_num_unpublished.return_value = 0
        builder.statemachine.set_to_waiting_to_be_available()
        testchannel = mock.MagicMock()

        # Run code to be tested:
        builder.on_channel_open(testchannel)

        # Check result:
        self.assertTrue(builder.statemachine.is_AVAILABLE())
        testchannel.confirm_delivery.assert_called()
        testchannel.add_on_return_callback.assert_called()
        testchannel.add_on_close_callback.assert_called()
        builder.thread.get_num_unpublished.assert_called()
        builder.thread.add_event_publish_message.assert_not_called()

    '''
    We test the actions when RabbitMQ has opened a channel,
    and some messages arrived in the meantime (from the publisher).
    The messages are sent.
    '''
    def test_on_channel_open_messages_waiting_ok(self):

        # Preparation:
        builder = self.make_builder()
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        builder.statemachine.set_to_waiting_to_be_available()
        builder.thread.get_num_unpublished.return_value = 11
        testchannel = mock.MagicMock()

        # Run code to be tested:
        builder.on_channel_open(testchannel)

        # Check result:
        self.assertTrue(builder.statemachine.is_AVAILABLE())
        testchannel.confirm_delivery.assert_called()
        testchannel.add_on_return_callback.assert_called()
        testchannel.add_on_close_callback.assert_called()
        builder.thread.get_num_unpublished.assert_called()
        builder.thread.add_event_publish_message.assert_called()
        builder.thread.add_event_publish_message.call_count >= 11


    '''
    We test the actions when RabbitMQ has opened a channel,
    but in the meantime the library was force-closed.
    No messages are sent, and the library is safety-closed.
    '''
    def test_on_channel_open_force_closed(self):

        # Preparation:
        builder = self.make_builder()
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        builder.statemachine.set_detail_closed_by_publisher()
        builder.statemachine.set_to_force_finished()
        testchannel = mock.MagicMock()

        # Run code to be tested:
        builder.on_channel_open(testchannel)

        # Check result:
        self.assertTrue(builder.statemachine.is_FORCE_FINISHED())
        self.assertTrue(builder.statemachine.get_detail_closed_by_publisher())
        builder.thread.add_event_publish_message.assert_not_called()
        builder.shutter.safety_finish.assert_called_with('closed before connection was ready. reclosing.')

    '''
    We test the actions when RabbitMQ has opened a channel,
    but in the meantime the library was force-closed.
    No messages are sent, although some were waiting, and
    the library is safety-closed.
    '''
    def test_on_channel_open_force_closed_pending_messages(self):

        # Preparation:
        builder = self.make_builder()
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        builder.statemachine.set_detail_closed_by_publisher()
        builder.statemachine.set_to_force_finished()
        builder.thread.get_num_unpublished.return_value = 11
        testchannel = mock.MagicMock()

        # Run code to be tested:
        builder.on_channel_open(testchannel)

        # Check result:
        self.assertTrue(builder.statemachine.is_PERMANENTLY_UNAVAILABLE())
        builder.thread.add_event_publish_message.assert_not_called()
        builder.shutter.safety_finish.assert_called_with('closed before connection was ready. reclosing.')

    '''
    We test the actions when RabbitMQ has opened a channel,
    but in the meantime the library was gently closed.
    '''
    def test_on_channel_open_gently_closed(self):

        # Preparation:
        builder = self.make_builder()
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        builder.statemachine.set_to_wanting_to_stop()
        testchannel = mock.MagicMock()

        # Run code to be tested:
        builder.on_channel_open(testchannel)

        # Check result:
        self.assertFalse(builder.statemachine.is_AVAILABLE())
        self.assertTrue(builder.statemachine.is_AVAILABLE_BUT_WANTS_TO_STOP())
        builder.thread.add_event_publish_message.assert_not_called()
        builder.shutter.safety_finish.assert_not_called()

    '''
    We test the actions when RabbitMQ has opened a channel,
    but in the meantime the library was gently closed.
    The pending messages are published.
    '''
    def test_on_channel_open_gently_closed_pending(self):

        # Preparation:
        builder = self.make_builder()
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        builder.statemachine.set_to_wanting_to_stop()
        builder.thread.get_num_unpublished.return_value = 11
        testchannel = mock.MagicMock()

        # Run code to be tested:
        builder.on_channel_open(testchannel)

        # Check result:
        self.assertTrue(builder.statemachine.is_AVAILABLE_BUT_WANTS_TO_STOP())
        testchannel.confirm_delivery.assert_called()
        testchannel.add_on_return_callback.assert_called()
        testchannel.add_on_close_callback.assert_called()
        builder.thread.get_num_unpublished.assert_called()
        builder.thread.add_event_publish_message.assert_called()
        builder.thread.add_event_publish_message.call_count >= 11
        builder.shutter.safety_finish.assert_not_called()

    #
    # on_connection_error
    #

    '''
    Testing behaviour on connection errors, if only one RabbitMQ
    instances has been tried, and more are available.
    It tries to connect to the next.
    '''
    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_on_connection_error_has_more(self, connpatch):

        # Preparation:
        builder = self.make_builder()
        builder.thread.get_num_unpublished.return_value = 11
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        builder.statemachine.set_to_waiting_to_be_available()

        # Mock the connection
        mock_connection = mock.MagicMock()
        def side_effect_add_timeout(wait_seconds, callback):
            callback()
        mock_connection.add_timeout.side_effect = side_effect_add_timeout

        # Run code to be tested:
        builder.on_connection_error(mock_connection, 'foo message')

        # Check result:
        # Reconnect was called:
        wait_seconds = 0
        mock_connection.add_timeout.assert_called_with(wait_seconds, builder.reconnect)
        # This was called inside reconnect:
        builder.thread._connection.ioloop.stop.assert_called()
        connpatch.assert_called()
        self.assertEquals(connpatch.call_count, 1)
        # As the connection is never built, no messages are sent, it is not set to available:
        builder.thread.add_event_publish_message.assert_not_called()
        self.assertFalse(builder.statemachine.is_AVAILABLE())

    '''
    Testing behaviour on connection errors, if there was a
    force-finish in the meantime.
    '''
    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_on_connection_error_force_finish(self, connpatch):

        # Preparation:
        builder = self.make_builder()
        builder.thread.get_num_unpublished.return_value = 11
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        builder.statemachine.set_to_force_finished()

        # Mock the connection
        mock_connection = mock.MagicMock()
        def side_effect_add_timeout(wait_seconds, callback):
            callback()
        mock_connection.add_timeout.side_effect = side_effect_add_timeout

        # Run code to be tested:
        with self.assertRaises(esgfpid.rabbit.exceptions.PIDServerException) as e:
            builder.on_connection_error(mock_connection, 'foo message')

        # Check result:
        # Reconnect was not called:
        mock_connection.add_timeout.assert_not_called()
        connpatch.assert_not_called()
        # As the connection is never built, no messages are sent, it is not set to available:
        builder.thread.add_event_publish_message.assert_not_called()
        self.assertFalse(builder.statemachine.is_AVAILABLE())
        self.assertTrue(builder.statemachine.is_FORCE_FINISHED())

    '''
    Testing behaviour on connection errors, if all RabbitMQ
    instances have been tried once.
    It is reset, all RabbitMQ instances are tried again after
    a waiting period.
    '''
    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_on_connection_error_none_left_reset(self, connpatch):

        # Preparation:
        builder = self.make_builder()
        builder.thread.get_num_unpublished.return_value = 11
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        builder._ConnectionBuilder__node_manager.set_next_host() # otherwise, we get errors, if no current host is available at all.
        builder._ConnectionBuilder__node_manager._NodeManager__trusted_nodes = {}
        builder.statemachine.set_to_waiting_to_be_available()

        # Mock the connection
        mock_connection = mock.MagicMock()
        def side_effect_add_timeout(wait_seconds, callback):
            callback()
        mock_connection.add_timeout.side_effect = side_effect_add_timeout

        # Run code to be tested:
        builder.on_connection_error(mock_connection, 'foo message')

        # Check result:
        # Reconnect was called:
        wait_seconds = esgfpid.defaults.RABBIT_RECONNECTION_SECONDS
        mock_connection.add_timeout.assert_called_with(wait_seconds, builder.reconnect)
        # This was called inside reconnect:
        builder.thread._connection.ioloop.stop.assert_called()
        connpatch.assert_called()
        self.assertEquals(connpatch.call_count, 1)
        # As the connection is never built, no messages are sent, it is not set to available:
        builder.thread.add_event_publish_message.assert_not_called()
        self.assertFalse(builder.statemachine.is_AVAILABLE())

    '''
    Testing behaviour on connection errors, if all RabbitMQ
    instances have been tried several times...
    It is set to permanently unavailable, no more reconnections.
    '''
    def test_on_connection_error_none_left_no_resetting(self):

        # Preparation:
        builder = self.make_builder()
        builder.thread.get_num_unpublished.return_value = 11
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        builder._ConnectionBuilder__node_manager.set_next_host() # otherwise, we get errors, if no current host is available at all.
        builder._ConnectionBuilder__node_manager._NodeManager__trusted_nodes = {}
        builder._ConnectionBuilder__reconnect_counter = esgfpid.defaults.RABBIT_RECONNECTION_MAX_TRIES
        builder.statemachine.set_to_waiting_to_be_available()

        # Run code to be tested:
        mock_connection = mock.MagicMock()
        with self.assertRaises(esgfpid.rabbit.exceptions.PIDServerException) as e:
            builder.on_connection_error(mock_connection, 'foo message')

        # Check result:
        self.assertIn('Permanently failed to connect to RabbitMQ.', str(e.exception))
        self.assertIn('Tried all hosts 3 times.', str(e.exception))
        self.assertIn('Giving up. No PID requests will be sent.', str(e.exception))
        mock_connection.add_timeout.assert_not_called()
        builder.thread.add_event_publish_message.assert_not_called()
        builder.thread.add_event_publish_message.call_count >= 0
        self.assertTrue(builder.statemachine.is_PERMANENTLY_UNAVAILABLE())
        self.assertTrue(builder.statemachine.detail_could_not_connect)

    def test_on_connection_error_force_finished_2(self):

        # Preparation:
        builder = self.make_builder()
        builder.thread.get_num_unpublished.return_value = 11
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()
        builder.statemachine.set_to_waiting_to_be_available()

        # Make sure the is_FORCE_FINISH returns False the first time and true the second time...
        builder.statemachine.is_FORCE_FINISHED = mock.MagicMock()
        builder.statemachine.is_FORCE_FINISHED.side_effect = [False, True, True, True]

        # Mock the connection
        mock_connection = mock.MagicMock()

        # Run code to be tested:
        with self.assertRaises(esgfpid.rabbit.exceptions.PIDServerException) as e:
            builder.on_connection_error(mock_connection, 'foo message')

        

        # Check result:
        self.assertIn('until received a force-finish. Giving up', str(e.exception))
        builder.statemachine.is_FORCE_FINISHED.assert_called()
        self.assertEquals(builder.statemachine.is_FORCE_FINISHED.call_count, 4)
        # Reconnect was called:
        mock_connection.add_timeout.assert_not_called()
        # As the connection is never built, no messages are sent, it is not set to available:
        builder.thread.add_event_publish_message.assert_not_called()
        self.assertFalse(builder.statemachine.is_AVAILABLE())

    #
    # on_channel_close
    #

    '''
    The channel was closed because the publisher closed
    the library.
    Requires no action. User-close will call the on_connection_close
    callback, so everything is handled there.
    '''
    def test_on_channel_closed_by_publisher(self):

        # Preparation:
        builder = self.make_builder()
        builder.statemachine.set_to_permanently_unavailable()
        builder.statemachine.set_detail_closed_by_publisher()

        # Run code to be tested:
        testchannel = None
        builder.on_channel_closed(testchannel, 111, 'foo')

        # Check result
        builder.thread._connection.close.assert_not_called()

    '''
    The channel was closed unexpectedly.
    This can be caused by any event from the RabbitMQ.
    So we trigger a reconnection (which is triggered by
    closing the connection).
    '''
    def test_on_channel_closed_unexpected_ok(self):

        # Preparation:
        builder = self.make_builder()
        builder.statemachine.set_to_available()
        builder.thread._connection = mock.MagicMock()

        # Run code to be tested:
        testchannel = None
        builder.on_channel_closed(testchannel, 111, 'foo')

        # Check result
        builder.thread._connection.close.assert_called_with()

    '''
    The channel was closed because the desired exchange did
    not exist.
    '''
    def test_on_channel_no_exchange(self):

        # Preparation:
        builder = self.make_builder()
        builder.statemachine.set_to_available()
        builder.thread._connection = mock.MagicMock()

        # Run code to be tested:
        testchannel = None
        builder.on_channel_closed(testchannel, 404, 'foo')

        # Check result
        builder.thread._connection.close.assert_not_called()
        builder.thread.change_exchange_name.assert_called_with(esgfpid.defaults.RABBIT_FALLBACK_EXCHANGE_NAME)
        builder.thread._connection.channel.assert_called()

    '''
    The channel was closed because the fallback exchange did
    not exist.
    We close the connection. This triggers reconnection to
    another host (not in this test, as there is not RabbitMQ
    to call a callback.)
    '''
    def test_on_channel_no_fallback_exchange(self):

        # Preparation:
        builder = self.make_builder()
        builder.statemachine.set_to_available()
        builder.thread._connection = mock.MagicMock()

        # Run code to be tested:
        testchannel = None
        builder.on_channel_closed(testchannel, 404, "NOT_FOUND - no exchange 'FALLBACK'")

        # Check result
        builder.thread._connection.close.assert_called()

    #
    # on_connection_close
    #

    def test_on_connection_closed_gentle_ok(self):

        # Preparation:
        builder = self.make_builder()
        builder.statemachine.set_to_available()

        # Run code to be tested:
        testconnection = None
        builder.on_connection_closed(testconnection, 999, 'foo(not reopen)foo')

        # Check result
        builder.thread._connection.ioloop.stop.assert_called_with()
        self.assertTrue(builder.statemachine.is_PERMANENTLY_UNAVAILABLE())

    def test_on_connection_closed_forced_ok(self):

        # Preparation:
        builder = self.make_builder()
        builder.statemachine.set_to_available()
        builder.statemachine.closed_by_publisher = False

        # Run code to be tested:
        testconnection = None
        builder.on_connection_closed(testconnection, 999, 'foo(forced finish)foo')

        # Check result
        builder.thread._connection.ioloop.stop.assert_called_with()
        self.assertTrue(builder.statemachine.is_PERMANENTLY_UNAVAILABLE())


    def test_on_connection_closed_unexpected_ok(self):

        # Preparation:
        builder = self.make_builder()
        builder.statemachine.set_to_available()
        builder.statemachine.closed_by_publisher = False
        builder._ConnectionBuilder__start_connect_time = datetime.datetime.now()

        # Run code to be tested:
        builder.on_connection_closed(builder.thread._connection, 111, '(foo)')

        # Check result
        builder.thread._connection.ioloop.stop.assert_not_called()
        self.assertTrue(builder.statemachine.is_WAITING_TO_BE_AVAILABLE())
        builder.thread._connection.add_timeout.assert_called_with(0, builder.reconnect)

    def test_on_connection_closed_permanent_error_ok(self):

        # Preparation:
        builder = self.make_builder()
        builder.statemachine.set_to_available()
        builder.statemachine.closed_by_publisher = False

        # Run code to be tested:
        testconnection = None
        builder.on_connection_closed(testconnection, 999, 'foo(permanent error)foo')

        # Check result
        builder.thread._connection.ioloop.stop.assert_called_with()
        self.assertTrue(builder.statemachine.is_PERMANENTLY_UNAVAILABLE())

    #
    # reconnect
    #

    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_reconnect_no_rescued_messages(self, connpatch):

        # Preparation:
        builder = self.make_builder()

        # Run code to be tested:
        builder.reconnect()

        # Check result:
        builder.thread._connection.ioloop.stop.assert_called_with()
        builder.thread.reset_delivery_number.assert_called_with()
        builder.thread.reset_unconfirmed_messages_and_delivery_tags.assert_called_with()
        builder.thread.send_many_messages.assert_not_called()
        connpatch.assert_called()


    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_reconnect_some_rescued_messages(self, connpatch):

        # Preparation:
        builder = self.make_builder()
        builder.thread.get_num_unpublished.return_value = 11
        builder.thread.get_unconfirmed_messages_as_list_copy_during_lifetime = mock.MagicMock()
        builder.thread.get_unconfirmed_messages_as_list_copy_during_lifetime.return_value = ['foo','bar','baz']

        # Run code to be tested:
        builder.reconnect()

        # Check result:
        builder.thread._connection.ioloop.stop.assert_called_with()
        builder.thread.reset_delivery_number.assert_called_with()
        builder.thread.reset_unconfirmed_messages_and_delivery_tags.assert_called_with()
        builder.thread.send_many_messages.assert_called()
        connpatch.assert_called()


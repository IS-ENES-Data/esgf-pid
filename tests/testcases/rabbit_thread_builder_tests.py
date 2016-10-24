import unittest
import mock
import logging
import os
import esgfpid.rabbit.connparams
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class ThreadBuilderTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ########## (%s)' % os.path.basename(__file__))

    def tearDown(self):
        LOGGER.info('#############################')

    def make_builder(self, args=None):

        # Mocked:
        self.cred = esgfpid.rabbit.connparams.get_credentials('userfoo', 'pwfoo')
        if args is None:
            args = {
                "urls_fallback":['bar.de', 'baz.de'],
                "url_preferred":'foo.de',
                "exchange_name":'fooexchange'
            }
        args['credentials'] = self.cred
        self.thread = mock.MagicMock()
        self.feeder = mock.MagicMock()
        self.acceptor = mock.MagicMock()
        self.shutter = mock.MagicMock()
        self.statemachine = esgfpid.rabbit.asynchronous.thread_statemachine.StateMachine()
        self.confirmer = esgfpid.rabbit.asynchronous.thread_confirmer.ConfirmReactor()
        builder = esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder(
            self.thread,
            self.statemachine,
            self.confirmer,
            self.feeder,
            self.acceptor,
            self.shutter,
            args)
        return builder

    def add_leftovers_to_confirmer(self):
        self.confirmer._ConfirmReactor__unconfirmed_delivery_tags = copy.copy(UNCONFIRMED_TAGS)
        self.confirmer._ConfirmReactor__unconfirmed_messages_dict = copy.copy(UNCONFIRMED_MESSAGES)

    # Tests

    #
    # Publish a message
    #

    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_build_connection_1_ok(self, connpatch):

        # Patch
        connpatch.return_value = mock.MagicMock()

        # Preparation:
        builder = self.make_builder()

        # Run code to be tested:
        builder.trigger_connection_to_rabbit_etc()
        # Tests:
        # trigger_connection_to_rabbit_etc
        # __connect_to_rabbit
        # __get_connection_params

        # Check result:
        self.assertEquals(connpatch.call_count, 1)

    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_rabbit_channel')
    def test_build_connection_2_on_connection_open_ok(self, channelpatch):

        # Patch
        channelpatch.return_value = mock.MagicMock()

        # Preparation:
        builder = self.make_builder()

        # Run code to be tested:
        builder.on_connection_open(None)
        # Tests:
        # on_connection_open
        # __add_on_connection_close_callback

        # Check result:
        self.assertEquals(channelpatch.call_count, 1)

    def test_build_connection_3_on_channel_open_ok(self):

        # Preparation:
        builder = self.make_builder()
        testchannel = mock.MagicMock()
        self.statemachine.set_to_waiting_to_be_available()

        # Run code to be tested:
        builder.on_channel_open(testchannel)
        # Tests:
        # on_channel_open
        # __add_on_channel_close_callback
        # __add_on_return_callback
        # __make_channel_confirm_delivery
        # __make_ready_for_publishing
        # __check_for_already_arrived_messages

        # Check result:
        self.assertTrue(self.statemachine.is_available_for_client_publishes())
        self.acceptor.trigger_publishing_n_messages_if_ok.assert_not_called()

    def test_build_connection_3_on_channel_open_messages_waiting_ok(self):

        # Preparation:
        builder = self.make_builder()
        testchannel = mock.MagicMock()
        self.statemachine.set_to_waiting_to_be_available()
        self.feeder.get_num_unpublished = mock.MagicMock(return_value=10)

        # Run code to be tested:
        builder.on_channel_open(testchannel)
        # Tests:
        # on_channel_open
        # __add_on_channel_close_callback
        # __add_on_return_callback
        # __make_channel_confirm_delivery
        # __make_ready_for_publishing
        # __check_for_already_arrived_messages

        # Check result:
        self.assertTrue(self.statemachine.is_available_for_client_publishes())
        self.acceptor.trigger_publishing_n_messages_if_ok.assert_called_with(10)

    def test_build_connection_3_on_channel_open_forceclosed(self):

        # Preparation:
        builder = self.make_builder()
        testchannel = mock.MagicMock()
        self.statemachine.closed_by_publisher = True
        self.statemachine.set_to_permanently_unavailable()

        # Run code to be tested:
        builder.on_channel_open(testchannel)
        # Tests:
        # on_channel_open
        # __add_on_channel_close_callback
        # __add_on_return_callback
        # __make_channel_confirm_delivery
        # __make_ready_for_publishing
        # __reclose_freshly_made_connection
        # __safety_finish

        # Check result:
        self.assertTrue(self.statemachine.is_permanently_unavailable())
        self.assertTrue(self.statemachine.closed_by_publisher)
        self.acceptor.trigger_publishing_n_messages_if_ok.assert_not_called()
        self.shutter.safety_finish.assert_called_with('closed before connection was ready. reclosing.')

    def test_build_connection_3_on_channel_open_forceclosed_pending_messages(self):

        # Preparation:
        builder = self.make_builder()
        testchannel = mock.MagicMock()
        self.statemachine.closed_by_publisher = True
        self.statemachine.set_to_permanently_unavailable()
        self.feeder.get_num_unpublished = mock.MagicMock(return_value=10)

        # Run code to be tested:
        builder.on_channel_open(testchannel)
        # Tests:
        # on_channel_open
        # __add_on_channel_close_callback
        # __add_on_return_callback
        # __make_channel_confirm_delivery
        # __make_ready_for_publishing
        # __reclose_freshly_made_connection
        # __safety_finish

        # Check result:
        self.assertTrue(self.statemachine.is_permanently_unavailable())
        self.acceptor.trigger_publishing_n_messages_if_ok.assert_not_called()
        self.shutter.safety_finish.assert_called_with('closed before connection was ready. reclosing.')

    def test_build_connection_3_on_channel_open_gentlyclosed(self):

        # Preparation:
        builder = self.make_builder()
        testchannel = mock.MagicMock()
        self.statemachine.set_to_wanting_to_stop()

        # Run code to be tested:
        builder.on_channel_open(testchannel)
        # Tests:
        # on_channel_open
        # __add_on_channel_close_callback
        # __add_on_return_callback
        # __make_channel_confirm_delivery
        # __make_ready_for_publishing
        # __check_for_already_arrived_messages

        # Check result:
        self.assertTrue(self.statemachine.is_available_but_wants_to_stop())
        self.acceptor.trigger_publishing_n_messages_if_ok.assert_not_called()
        self.shutter.safety_finish.assert_not_called()

    def test_build_connection_3_on_channel_open_gentlyclosed_pending(self):

        # Preparation:
        builder = self.make_builder()
        testchannel = mock.MagicMock()
        self.statemachine.set_to_wanting_to_stop()
        self.feeder.get_num_unpublished = mock.MagicMock(return_value=10)

        # Run code to be tested:
        builder.on_channel_open(testchannel)
        # Tests:
        # on_channel_open
        # __add_on_channel_close_callback
        # __add_on_return_callback
        # __make_channel_confirm_delivery
        # __make_ready_for_publishing
        # __check_for_already_arrived_messages

        # Check result:
        self.assertTrue(self.statemachine.is_available_but_wants_to_stop())
        self.acceptor.trigger_publishing_n_messages_if_ok.assert_called_with(10)
        self.shutter.safety_finish.assert_not_called()

    def test_on_connection_error_none_left(self):

        # Preparation:
        args = {
            "urls_fallback":[],
            "url_preferred":'foo.de',
            "exchange_name":'fooexchange'
        }
        builder = self.make_builder(args)
        # To avoid that it tries to reconnect some more times:
        builder._ConnectionBuilder__reconnect_counter = esgfpid.defaults.RABBIT_ASYN_RECONNECTION_MAX_TRIES
        self.statemachine.set_to_waiting_to_be_available()

        # Run code to be tested:
        connection = mock.MagicMock()
        builder.on_connection_error(connection, 'foo message')
        # Tests:
        # on_connection_error
        # __is_fallback_url_left
        # __change_state_to_permanently_could_not_connect
        # __inform_permanently_could_not_connect

        # Check result:
        self.assertTrue(self.statemachine.is_permanently_unavailable())
        self.assertTrue(self.statemachine.could_not_connect)
        self.acceptor.trigger_publishing_n_messages_if_ok.assert_not_called()

    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__please_open_connection')
    def test_on_connection_error_urls_left(self, pleasepatch):

        # Preparation:
        builder = self.make_builder()
        args = {
            "urls_fallback":['foobarbaz'],
            "url_preferred":'foo.de',
            "exchange_name":'fooexchange'
        }
        builder = self.make_builder(args)
        #builder.RABBIT_HOSTS = ['foobarbaz']
        pleasepatch.return_value = 'mock_connection'
        self.statemachine.set_to_waiting_to_be_available()

        # Run code to be tested:
        builder.on_connection_error(None, 'foo message')
        # Tests:
        # on_connection_error
        # __is_fallback_url_left
        # __change_state_to_permanently_could_not_connect
        # __inform_permanently_could_not_connect

        # Check result:
        self.assertEquals('mock_connection', self.thread._connection)
        self.assertEquals(builder.CURRENT_HOST, 'foobarbaz')
        self.assertTrue(self.statemachine.is_waiting_to_be_available())
        self.acceptor.trigger_publishing_n_messages_if_ok.assert_not_called()


    #
    # on_channel_close and on_connection_close
    #

    def test_on_channel_closed_unexpected(self):

        # Preparation:
        builder = self.make_builder()
        self.statemachine.set_to_permanently_unavailable()
        self.statemachine.closed_by_publisher = True

        # Run code to be tested:
        connection = mock.MagicMock()
        builder.on_channel_closed(connection, 111, 'foo')

        # Check result
        self.thread._connection.close.assert_not_called()

    def test_on_channel_closed_ok(self):

        # Preparation:
        builder = self.make_builder()
        self.statemachine.set_to_available()
        self.statemachine.closed_by_publisher = False
        self.thread._connection = mock.MagicMock()
        self.thread._connection.close.return_value = mock.MagicMock()

        # Run code to be tested:
        builder.on_channel_closed(None, 111, 'foo')

        # Check result
        self.thread._connection.close.assert_called_with()

    def test_on_connection_closed_gentle_ok(self):

        # Preparation:
        builder = self.make_builder()
        self.statemachine.set_to_available()
        self.statemachine.closed_by_publisher = False
        self.thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER=999
        self.thread.ERROR_TEXT_CONNECTION_FORCE_CLOSED='(forced finish)'
        self.thread.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN='(not reopen)'
        #self.thread._connection = mock.MagicMock()
        #self.thread._connection.ioloop = mock.MagicMock()
        #self.thread._connection.ioloop.stop.return_value = mock.MagicMock()
        #self.thread._connection.close.return_value = mock.MagicMock()

        # Run code to be tested:
        builder.on_connection_closed(None, 999, 'foo(not reopen)foo')

        # Check result
        self.thread._connection.ioloop.stop.assert_called_with()
        self.assertTrue(self.statemachine.is_permanently_unavailable())

    def test_on_connection_closed_forced_ok(self):

        # Preparation:
        builder = self.make_builder()
        self.statemachine.set_to_available()
        self.statemachine.closed_by_publisher = False
        self.thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER=999
        self.thread.ERROR_TEXT_CONNECTION_FORCE_CLOSED='(forced finish)'
        self.thread.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN='(not reopen)'
        #self.thread._connection.ioloop.stop.return_value = mock.MagicMock()

        # Run code to be tested:
        builder.on_connection_closed(None, 999, 'foo(forced finish)foo')

        # Check result
        self.thread._connection.ioloop.stop.assert_called_with()
        self.assertTrue(self.statemachine.is_permanently_unavailable())


    def test_on_connection_closed_unexpected_ok(self):

        # Preparation:
        builder = self.make_builder()
        self.statemachine.set_to_available()
        self.statemachine.closed_by_publisher = False
        self.thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER=999
        self.thread.ERROR_TEXT_CONNECTION_FORCE_CLOSED='(forced finish)'
        self.thread.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN='(not reopen)'

        # Run code to be tested:
        builder.on_connection_closed(self.thread._connection, 111, '(foo)')

        # Check result
        self.thread._connection.ioloop.stop.assert_not_called()
        self.assertTrue(self.statemachine.is_waiting_to_be_available())
        self.thread._connection.add_timeout.assert_called_with(5, builder.reconnect)

    #
    # reconnect
    #

    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__connect_to_rabbit')
    def test_reconnect_no_rescued_messages(self, connpatch):

        # Preparation:
        builder = self.make_builder()

        # Run code to be tested:
        builder.reconnect()
        # Tests:


        # Check result:
        self.feeder.reset_message_number.assert_called_with()
        self.acceptor.send_many_messages.assert_not_called()
        self.assertEquals(connpatch.call_count, 1)
        builder.thread._connection.ioloop.start.assert_any_call()
        builder.thread._connection.ioloop.stop.assert_any_call()


    @mock.patch('esgfpid.rabbit.asynchronous.thread_builder.ConnectionBuilder._ConnectionBuilder__connect_to_rabbit')
    def test_reconnect_some_rescued_messages(self, connpatch):

        # Preparation:
        builder = self.make_builder()
        self.confirmer.put_to_unconfirmed_delivery_tags(1)
        self.confirmer.put_to_unconfirmed_delivery_tags(2)
        self.confirmer.put_to_unconfirmed_delivery_tags(3)
        self.confirmer.put_to_unconfirmed_messages_dict(1,'foo1')
        self.confirmer.put_to_unconfirmed_messages_dict(2,'foo2')
        self.confirmer.put_to_unconfirmed_messages_dict(3,'foo3')

        # Run code to be tested:
        builder.reconnect()

        # Check result:
        self.feeder.reset_message_number.assert_called_with()
        self.acceptor.send_many_messages.assert_called_with(['foo1','foo3','foo2'])
        self.assertEquals(connpatch.call_count, 1)
        builder.thread._connection.ioloop.start.assert_any_call()
        builder.thread._connection.ioloop.stop.assert_any_call()

    #
    # ioloop
    #

    def test_start_ioloop_immediately(self):

        # Preparation:
        builder = self.make_builder()
        builder.thread._connection = mock.MagicMock()
        builder.thread._connection.is_open = True # will succeed

        # Run code to be tested:
        builder.start_ioloop_when_connection_ready(20,20)

        # Check result:
        builder.thread._connection.ioloop.start.assert_any_call()

    def test_start_ioloop_give_up(self):

        # Preparation:
        builder = self.make_builder()
        self.statemachine.could_not_connect = True # will give up!
        self.statemachine.set_to_permanently_unavailable()
        builder.thread._connection = mock.MagicMock()
        builder.thread._connection.is_open = True

        # Run code to be tested:
        builder.start_ioloop_when_connection_ready(20,20)

        # Check result:
        builder.thread._connection.ioloop.start.assert_not_called()

    def test_start_ioloop_second_try(self):

        # Preparation:
        builder = self.make_builder()
        builder.thread._connection.side_effect = [None, mock.MagicMock]
        #builder.thread._connection.is_open = []

        # Run code to be tested:
        builder.start_ioloop_when_connection_ready(20,20)

        # Check result:
        builder.thread._connection.ioloop.start.assert_any_call()


    def test_start_ioloop_loop_on(self):

        # Preparation:
        builder = self.make_builder()
        builder.thread._connection = None

        # Run code to be tested:
        with self.assertRaises(ValueError):
            builder.start_ioloop_when_connection_ready(3,0.1)


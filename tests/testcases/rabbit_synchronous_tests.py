import unittest
import mock
import logging
import json
import sys
sys.path.append("..")
import esgfpid.rabbit
import tests.mocks.pikamock
import pika.exceptions
import pika

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class RabbitConnectorTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')

    def tearDown(self):
        LOGGER.info('#############################')

    def __get_args_dict(self):

        cred = esgfpid.rabbit.connparams.get_credentials('johndoe','pass123')
        args = dict(
            exchange_name = 'exch',
            url_preferred = 'first.host',
            urls_fallback = ['fallback.host', 'fallback.host.too'],
            credentials = cred
        )
        return args

    # Tests

    #
    # Setup of the connection/channel:
    #

    def test_init_ok(self):

        # Test variables:
        args = self.__get_args_dict()

        # Run code to be tested:
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)

        # Check result
        self.assertIsInstance(testrabbit, esgfpid.rabbit.synchronous.SynchronousServerConnector)
        # Communcation not established yet:
        established = testrabbit._SynchronousServerConnector__communication_established
        self.assertFalse(established)

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_open_rabbit_connection_ok(self, connectionmock):

        # Define replacement for mocked object:
        def make_mocked_connection(params):
            return tests.mocks.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)
        
        # Run code to be tested:
        testrabbit.open_rabbit_connection()

        # Check result:
        # Connection created:
        conn = testrabbit._SynchronousServerConnector__connection
        self.assertIsInstance(conn, tests.mocks.pikamock.MockPikaBlockingConnection)
        # Channel created:
        channel = testrabbit._SynchronousServerConnector__channel
        self.assertIsInstance(channel, tests.mocks.pikamock.MockChannel)
        # Communcation established:
        established = testrabbit._SynchronousServerConnector__communication_established
        self.assertTrue(established)
        # Connected to preferred URL:
        expected_hosts = args['url_preferred']
        self.assertIn(conn.host, expected_hosts, 'Connected to: %s' % conn.host)

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_open_rabbit_connection_not_ok(self, connectionmock):
        '''
        All URLs will cause ProbableAuthenticationErrors,
        so no connection can be opened.
        '''

        # Define replacement for mocked object:
        connectionmock.side_effect = pika.exceptions.AuthenticationError

        # Make test rabbit:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)
        
        # Run code to be tested:
        testrabbit.open_rabbit_connection()

        # Check result:
        # Connection is None:
        conn = testrabbit._SynchronousServerConnector__connection
        self.assertIsNone(conn)
        # Channel is None:
        channel = testrabbit._SynchronousServerConnector__channel
        self.assertIsNone(channel)
        # Communcation not established:
        established = testrabbit._SynchronousServerConnector__communication_established
        self.assertFalse(established)

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_open_rabbit_connection_first_fails_auth(self, connectionmock):
        '''
        The first URL causes a ProbableAuthenticationError,
        the fallback URL works, so the connection will be ok.
        '''

        # Define replacement for mocked object:
        def make_mocked_connection(params):
            if params.host == 'first.host':
                raise pika.exceptions.ProbableAuthenticationError
            else:
                return tests.mocks.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)

        # Run code to be tested:
        testrabbit.open_rabbit_connection()

        # Check result:
        # Connection created:
        conn = testrabbit._SynchronousServerConnector__connection
        self.assertIsInstance(conn, tests.mocks.pikamock.MockPikaBlockingConnection)
        # Channel created:
        channel = testrabbit._SynchronousServerConnector__channel
        self.assertIsInstance(channel, tests.mocks.pikamock.MockChannel)
        # Communcation established:
        established = testrabbit._SynchronousServerConnector__communication_established
        self.assertTrue(established)
        # Connected to fallback URL:
        expected_hosts = args['urls_fallback']
        self.assertIn(conn.host, expected_hosts, 'Connected to: %s' % conn.host)


    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_open_rabbit_connection_first_fails_conn(self, connectionmock):
        '''
        The first URL causes a AMQPConnectionError,
        the fallback URL works, so the connection will be ok.
        '''

        # Define replacement for mocked object:
        def make_mocked_connection(params):
            if params.host == 'first.host':
                raise pika.exceptions.AMQPConnectionError
            else:
                return tests.mocks.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)

        # Run code to be tested:
        testrabbit.open_rabbit_connection()

        # Check result:
        # Connection created:
        conn = testrabbit._SynchronousServerConnector__connection
        self.assertIsInstance(conn, tests.mocks.pikamock.MockPikaBlockingConnection)
        # Channel created:
        channel = testrabbit._SynchronousServerConnector__channel
        self.assertIsInstance(channel, tests.mocks.pikamock.MockChannel)
        # Communcation established:
        established = testrabbit._SynchronousServerConnector__communication_established
        self.assertTrue(established)
        # Connected to fallback URL:
        expected_hosts = args['urls_fallback']
        self.assertIn(conn.host, expected_hosts, 'Connected to: %s' % conn.host)

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_open_rabbit_connection_first_two_fail(self, connectionmock):
        '''
        The first two URLs cause a AMQPConnectionError,
        the last fallback URL works, so the connection will be ok.
        '''

        # Define replacement for mocked object:
        def make_mocked_connection(params):
            if params.host == 'first.host' or params.host == 'fallback.host.too':
                raise pika.exceptions.AMQPConnectionError
            else:
                return tests.mocks.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)

        # Run code to be tested:
        testrabbit.open_rabbit_connection()

        # Check result:
        # Connection created:
        conn = testrabbit._SynchronousServerConnector__connection
        self.assertIsInstance(conn, tests.mocks.pikamock.MockPikaBlockingConnection)
        # Channel created:
        channel = testrabbit._SynchronousServerConnector__channel
        self.assertIsInstance(channel, tests.mocks.pikamock.MockChannel)
        # Communcation established:
        established = testrabbit._SynchronousServerConnector__communication_established
        self.assertTrue(established)
        # Connected to fallback URL:
        expected_hosts = args['urls_fallback']
        self.assertIn(conn.host, expected_hosts, 'Connected to: %s' % conn.host)

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_channel')
    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_open_rabbit_channel_fails(self, connectionmock, channelmock):

        # Define replacement for mocked object:
        # Mock connection:
        def make_mocked_connection(params):
            return tests.mocks.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection
        # Mock channel:
        channelmock.side_effect = pika.exceptions.ChannelClosed

        # Make test rabbit:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)
        
        # Run code to be tested:
        testrabbit.open_rabbit_connection()

        # Check result:
        # Connection created:
        conn = testrabbit._SynchronousServerConnector__connection
        self.assertIsInstance(conn, tests.mocks.pikamock.MockPikaBlockingConnection)
        # Channel is None:
        channel = testrabbit._SynchronousServerConnector__channel
        self.assertIsNone(channel)
        # Communcation NOT established:
        established = testrabbit._SynchronousServerConnector__communication_established
        self.assertFalse(established)
        # Connected to fallback URL:
        expected_hosts = args['urls_fallback']
        self.assertIn(conn.host, expected_hosts, 'Connected to: %s' % conn.host)

    #
    # Close connection
    #

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_close_rabbit_connection_ok(self, connectionmock):

        # Define replacement for mocked object:
        def make_mocked_connection(params):
            return tests.mocks.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit with connection:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)
        testrabbit.open_rabbit_connection()

        # Run code to be tested:
        testrabbit.close_rabbit_connection()

        # Check result:
        # Connection should be closed:
        conn = testrabbit._SynchronousServerConnector__connection
        self.assertFalse(conn.is_open)

    #
    # Send message
    #

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_send_message_ok(self, connectionmock):
        '''
        We expect the message to arrive in the MockChannel
        on the first try.
        '''

        # Test variables:
        key = 'mykey'
        msg = {"foo":"bar", "ROUTING_KEY":key}

        # Define replacement for mocked object:
        def make_mocked_connection(params):
            return tests.mocks.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit with connection:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)
        testrabbit.open_rabbit_connection()

        # Run code to be tested:
        testrabbit.send_message_to_queue(msg)

        # Check result:
        # Message was sent:
        channel = testrabbit._SynchronousServerConnector__channel
        self.assertEquals(channel.success_counter, 1, 'Message should succeed!')
        # Message was sent on first try:
        self.assertEqual(channel.publish_counter, 1,
            'Message was sent %i times!' % channel.publish_counter)
        # Message arrived in mock channel:
        self.assertIn(json.dumps(msg), channel.messages,
            'Message %s not found in channel\'s list: %s' % (msg, channel.messages))
        self.assertIn(key, channel.routing_keys,
            'Routing key %s not found in channel\'s list: %s' % (key, channel.routing_keys))


    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_send_message_fail(self, connectionmock):

        # Test variables:
        key = 'mykey'
        msg = {"foo":"bar", "ROUTING_KEY":key}

        # Define replacement for mocked function:
        def make_mocked_connection(params):
            return tests.mocks.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit with connection:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)
        testrabbit.open_rabbit_connection()

        # Make publish raise an UnroutableError
        channel = testrabbit._SynchronousServerConnector__channel
        channel.num_failures = float('inf') # always failure!

        # Run code to be tested:
        testrabbit.send_message_to_queue(msg)

        # Check result:
        self.assertEquals(channel.success_counter, 0, 'Message should not succeed!')



    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_send_message_unroutable(self, connectionmock):

        # Test variables:
        key = 'mykey'
        msg = {"foo":"bar", "ROUTING_KEY":key}

        # Define replacement for mocked object:
        def make_mocked_connection(params):
            return tests.mocks.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection
        # Make basic_publish raise an error
        #publishmock.side_effect = pika.exceptions.UnroutableError('foo bar')

        # Make test rabbit with connection:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)
        testrabbit.open_rabbit_connection()

        # Make publish raise an UnroutableError
        channel = testrabbit._SynchronousServerConnector__channel
        channel.num_unroutables = float('inf') # always unroutable!

        # Run code to be tested:
        testrabbit.send_message_to_queue(msg)

        # Check result:
        self.assertEquals(channel.success_counter, 0, 'Message should not succeed!')

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_send_message_first_fails_then_ok(self, connectionmock):
        '''
        We make the channel return False (failure), then True (success).
        We check if the message arrived in the MockChannel.
        '''

        # Test variables:
        key = 'mykey'
        msg = {"foo":"bar", "ROUTING_KEY":key}

        # Define replacement for mocked object:
        def make_mocked_connection(params):
            return tests.mocks.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit with connection:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)
        testrabbit.open_rabbit_connection()

        # Make publish succeed only in 2nd try:
        channel = testrabbit._SynchronousServerConnector__channel
        channel.num_failures = 1

        # Run code to be tested:
        testrabbit.send_message_to_queue(msg)

        # Check result:
        # Message was sent:
        self.assertEquals(channel.success_counter, 1, 'Message should succeed!')
        # Message was sent on second try:
        self.assertEqual(channel.publish_counter, 2,
            'Message was sent %i times!' % channel.publish_counter)
        # Message arrived in mock channel:
        self.assertIn(json.dumps(msg), channel.messages,
            'Message %s not found in channel\'s list: %s' % (msg, channel.messages))
        self.assertIn(key, channel.routing_keys,
            'Routing key %s not found in channel\'s list: %s' % (key, channel.routing_keys))

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousServerConnector._SynchronousServerConnector__make_connection')
    def test_send_message_no_connection(self, connectionmock):
        '''
        If there is no connection, it is built before publishing
        the message.
        We check if the message arrived in the MockChannel.
        '''

        # Test variables:
        key = 'mykey'
        msg = {"foo":"bar", "ROUTING_KEY":key}

        # Define replacement for mocked object:
        def make_mocked_connection(params):
            return tests.mocks.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit without connection:
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)

        # Run code to be tested:
        testrabbit.send_message_to_queue(msg)

        # Check result:
        # Connection is opened:
        conn = testrabbit._SynchronousServerConnector__connection
        self.assertTrue(conn.is_open)
        # Message was sent:
        channel = testrabbit._SynchronousServerConnector__channel
        self.assertEquals(channel.success_counter, 1, 'Message should succeed!')
        # Message was sent on first try:
        self.assertEqual(channel.publish_counter, 1,
            'Message was sent %i times!' % channel.publish_counter)
        # Message arrived in mock channel:
        self.assertIn(json.dumps(msg), channel.messages,
            'Message %s not found in channel\'s list: %s' % (msg, channel.messages))
        self.assertIn(key, channel.routing_keys,
            'Routing key %s not found in channel\'s list: %s' % (key, channel.routing_keys))
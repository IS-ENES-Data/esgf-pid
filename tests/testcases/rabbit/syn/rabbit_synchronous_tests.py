import unittest
import mock
import logging
import json
import tests.resources.pikamock
import tests.main_test_script
import pika.exceptions
import pika
import tests.globalvar

import esgfpid.rabbit
from esgfpid.rabbit.exceptions import PIDServerException

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

# Skip slow tests?
import globalvar
if globalvar.QUICK_ONLY:
    print('Skipping slow tests in module "%s".' % __name__)


class RabbitConnectorTestCase(unittest.TestCase):

    slow_message = '\nRunning a slow test (avoid by using -ls flag).'


    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)
    
    def tearDown(self):
        LOGGER.info('#############################')

    #
    # Setup of the connection/channel:
    #

    def test_init_ok(self):

        # Test variables:
        nodemanager = TESTHELPERS.get_nodemanager()

        # Run code to be tested:
        testrabbit = esgfpid.rabbit.synchronous.SynchronousRabbitConnector(nodemanager)

        # Check results:
        # Instance is created:
        self.assertIsInstance(testrabbit, esgfpid.rabbit.synchronous.SynchronousRabbitConnector)
        # Communcation not established yet:
        established = testrabbit._SynchronousRabbitConnector__communication_established
        self.assertFalse(established)

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
    def test_open_rabbit_connection_ok(self, connectionmock):

        # Define replacement for mocked method:
        # Using side_effect instead of return_value, so we can use the params object passed as arg!
        def make_mocked_connection(params):
            return tests.resources.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit:
        testrabbit = TESTHELPERS.get_synchronous_rabbit()
        
        # Run code to be tested (during this, the connection is opened, i.e. the patch is needed)
        testrabbit.open_rabbit_connection()

        # Check result:
        # Connection created:
        conn = testrabbit._SynchronousRabbitConnector__connection
        self.assertIsInstance(conn, tests.resources.pikamock.MockPikaBlockingConnection)
        # Channel created:
        channel = testrabbit._SynchronousRabbitConnector__channel
        self.assertIsInstance(channel, tests.resources.pikamock.MockChannel)
        # Communcation established:
        established = testrabbit._SynchronousRabbitConnector__communication_established
        self.assertTrue(established)
        # Connected to preferred URL:
        expected_host = RABBIT_URL_TRUSTED
        self.assertIn(conn.host, expected_host, 'Connected to: %s instead of %s' % (conn.host, expected_host))


    @unittest.skipIf(globalvar.QUICK_ONLY, '(this test is slow)')
    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
    def test_open_rabbit_connection_not_ok(self, connectionmock):

        print(self.slow_message)

        # Define replacement for mocked method:
        # All connection attempts fail with an Authentication Error.
        connectionmock.side_effect = pika.exceptions.AuthenticationError

        # Make test rabbit:
        testrabbit = TESTHELPERS.get_synchronous_rabbit()
        
        # Run code to be tested (during this, the connection is opened, i.e. the patch is needed)
        with self.assertRaises(PIDServerException) as e:
            testrabbit.open_rabbit_connection()

        # Check result:
        # Connection is None:
        conn = testrabbit._SynchronousRabbitConnector__connection
        self.assertIsNone(conn)
        # Channel is None:
        channel = testrabbit._SynchronousRabbitConnector__channel
        self.assertIsNone(channel)
        # Communcation not established:
        established = testrabbit._SynchronousRabbitConnector__communication_established
        self.assertFalse(established)


    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
    def test_open_rabbit_connection_first_fails_auth(self, connectionmock):

        # Define replacement for mocked object:
        # RabbitMQ with priority 1 fails, the one with
        # priority 2 succeeds.
        def make_mocked_connection(params):
            if params.host == RABBIT_URL_TRUSTED:
                raise pika.exceptions.ProbableAuthenticationError
            elif params.host == RABBIT_URL_TRUSTED2:
                return tests.resources.pikamock.MockPikaBlockingConnection(params)
            else:
                raise ValueError("Unexpected RabbitMQ URL. Unit tests cannot work properly if the wrong test data is passed.")
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit:
        testrabbit = TESTHELPERS.get_synchronous_rabbit()

        # Run code to be tested (during this, the connection is opened, i.e. the patch is needed)
        testrabbit.open_rabbit_connection()

        # Check result:
        # Connection created:
        conn = testrabbit._SynchronousRabbitConnector__connection
        self.assertIsInstance(conn, tests.resources.pikamock.MockPikaBlockingConnection)
        # Channel created:
        channel = testrabbit._SynchronousRabbitConnector__channel
        self.assertIsInstance(channel, tests.resources.pikamock.MockChannel)
        # Communcation established:
        established = testrabbit._SynchronousRabbitConnector__communication_established
        self.assertTrue(established)
        # Connected to fallback URL:
        expected_host = RABBIT_URL_TRUSTED2
        self.assertIn(conn.host, expected_host, 'Connected to: %s instead of %s' % (conn.host, expected_host))


    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
    def test_open_rabbit_connection_first_fails_conn(self, connectionmock):

        # Define replacement for mocked method:
        # RabbitMQ with priority 1 fails, the one with
        # priority 2 succeeds.
        def make_mocked_connection(params):
            if params.host == RABBIT_URL_TRUSTED:
                raise pika.exceptions.AMQPConnectionError
            elif params.host == RABBIT_URL_TRUSTED2:
                return tests.resources.pikamock.MockPikaBlockingConnection(params)
            else:
                raise ValueError("Unexpected RabbitMQ URL. Unit tests cannot work properly if the wrong test data is passed.")
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit:
        testrabbit = TESTHELPERS.get_synchronous_rabbit()

        # Run code to be tested (during this, the connection is opened, i.e. the patch is needed)
        testrabbit.open_rabbit_connection()

        # Check result:
        # Connection created:
        conn = testrabbit._SynchronousRabbitConnector__connection
        self.assertIsInstance(conn, tests.resources.pikamock.MockPikaBlockingConnection)
        # Channel created:
        channel = testrabbit._SynchronousRabbitConnector__channel
        self.assertIsInstance(channel, tests.resources.pikamock.MockChannel)
        # Communcation established:
        established = testrabbit._SynchronousRabbitConnector__communication_established
        self.assertTrue(established)
        # Connected to fallback URL:
        expected_host = RABBIT_URL_TRUSTED2
        self.assertIn(conn.host, expected_host, 'Connected to: %s instead of %s' % (conn.host, expected_host))

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
    def test_open_rabbit_connection_first_two_fail(self, connectionmock):

        # Define replacement for mocked method:
        # RabbitMQ with priority 1&2 fail, the third one succeeds.
        def make_mocked_connection(params):
            if params.host == RABBIT_URL_TRUSTED or params.host == RABBIT_URL_TRUSTED2:
                raise pika.exceptions.AMQPConnectionError
            elif params.host == RABBIT_URL_TRUSTED3:
                return tests.resources.pikamock.MockPikaBlockingConnection(params)
            else:
                raise ValueError("Unexpected RabbitMQ URL. Unit tests cannot work properly if the wrong test data is passed.")
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit:
        testrabbit = TESTHELPERS.get_synchronous_rabbit()

        # Run code to be tested (during this, the connection is opened, i.e. the patch is needed)
        testrabbit.open_rabbit_connection()

        # Check result:
        # Connection created:
        conn = testrabbit._SynchronousRabbitConnector__connection
        self.assertIsInstance(conn, tests.resources.pikamock.MockPikaBlockingConnection)
        # Channel created:
        channel = testrabbit._SynchronousRabbitConnector__channel
        self.assertIsInstance(channel, tests.resources.pikamock.MockChannel)
        # Communcation established:
        established = testrabbit._SynchronousRabbitConnector__communication_established
        self.assertTrue(established)
        # Connected to fallback URL:
        expected_host = RABBIT_URL_TRUSTED3
        self.assertIn(conn.host, expected_host, 'Connected to: %s instead of %s' % (conn.host, expected_host))

    @unittest.skipIf(globalvar.QUICK_ONLY, '(this test is slow)')
    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_channel')
    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
    def test_open_rabbit_channel_fails(self, connectionmock, channelmock):

        print(self.slow_message)

        # Define replacement for mocked methods:
        # Connection succeeds, channel does not.
        def make_mocked_connection(params):
            return tests.resources.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection
        channelmock.side_effect = pika.exceptions.ChannelClosed

        # Make test rabbit:
        testrabbit = TESTHELPERS.get_synchronous_rabbit()

        # Run code to be tested (during this, the connection is opened, i.e. the patch is needed)
        with self.assertRaises(PIDServerException) as e:
            testrabbit.open_rabbit_connection()

        # Check result:
        # Connection created:
        conn = testrabbit._SynchronousRabbitConnector__connection
        self.assertIsInstance(conn, tests.resources.pikamock.MockPikaBlockingConnection)
        # Channel is None:
        channel = testrabbit._SynchronousRabbitConnector__channel
        self.assertIsNone(channel)
        # Communcation NOT established:
        established = testrabbit._SynchronousRabbitConnector__communication_established
        self.assertFalse(established)
        # Connected to fallback URL:
        expected_host = RABBIT_URL_TRUSTED3
        self.assertIn(conn.host, expected_host, 'Connected to: %s instead of %s' % (conn.host, expected_host))

    #
    # Close connection
    #

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
    def test_close_rabbit_connection_ok(self, connectionmock):

        # Define replacement for mocked object:
        def make_mocked_connection(params):
            return tests.resources.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit with open connection:
        #(during this, the connection is opened, i.e. the patch is needed)
        testrabbit = TESTHELPERS.get_synchronous_rabbit()
        testrabbit.open_rabbit_connection()

        # Run code to be tested:
        testrabbit.close_rabbit_connection()

        # Check result:
        # Connection should be closed:
        conn = testrabbit._SynchronousRabbitConnector__connection
        self.assertFalse(conn.is_open)

    #
    # Send message
    #

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
    def test_send_message_ok(self, connectionmock):
        '''
        We expect the message to arrive in the MockChannel
        on the first try.
        '''

        # Test variables:
        key = 'mykey'
        msg = {"foo":"bar", "ROUTING_KEY":key}

        # Define replacement for mocked method:
        # A mocked connection is created.
        def make_mocked_connection(params):
            return tests.resources.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit with connection:
        testrabbit = TESTHELPERS.get_synchronous_rabbit()
        testrabbit.open_rabbit_connection()

        # Run code to be tested:
        testrabbit.send_message_to_queue(msg)

        # Check result:
        # Message was sent:
        channel = testrabbit._SynchronousRabbitConnector__channel
        self.assertEquals(channel.success_counter, 1, 'Message should succeed!')
        # Message was sent on first try:
        self.assertEqual(channel.publish_counter, 1,
            'Message was sent %i times!' % channel.publish_counter)
        # Message arrived in mock channel:
        self.assertIn(json.dumps(msg), channel.messages,
            'Message %s not found in channel\'s list: %s' % (msg, channel.messages))
        self.assertIn(key, channel.routing_keys,
            'Routing key %s not found in channel\'s list: %s' % (key, channel.routing_keys))

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
    def test_send_message_fail(self, connectionmock):

        # Test variables:
        key = 'mykey'
        msg = {"foo":"bar", "ROUTING_KEY":key}

        # Define replacement for mocked method:
        # A mocked connection is created.
        def make_mocked_connection(params):
            return tests.resources.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit with connection:
        testrabbit = TESTHELPERS.get_synchronous_rabbit()
        testrabbit.open_rabbit_connection()

        # Make publish raise an UnroutableError
        channel = testrabbit._SynchronousRabbitConnector__channel
        channel.num_failures = float('inf') # always failure!

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.MessageNotDeliveredException) as e:
            testrabbit.send_message_to_queue(msg)

        # Check result:
        self.assertEquals(channel.success_counter, 0, 'Message should not succeed!')

    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
    def test_send_message_unroutable(self, connectionmock):

        # Test variables:
        key = 'mykey'
        msg = {"foo":"bar", "ROUTING_KEY":key}

        # Define replacement for mocked object:
        def make_mocked_connection(params):
            return tests.resources.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection
        # Make basic_publish raise an error
        #publishmock.side_effect = pika.exceptions.UnroutableError('foo bar')

        # Make test rabbit with connection:
        testrabbit = TESTHELPERS.get_synchronous_rabbit()
        testrabbit.open_rabbit_connection()

        # Make publish raise an UnroutableError
        channel = testrabbit._SynchronousRabbitConnector__channel
        channel.num_unroutables = float('inf') # always unroutable!

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.MessageNotDeliveredException) as e:
            testrabbit.send_message_to_queue(msg)

        # Check result:
        self.assertEquals(channel.success_counter, 0, 'Message should not succeed!')
        self.assertEquals(channel.publish_counter, 2, 'Message was tried %i times, not 2.' % channel.publish_counter)



    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
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
            return tests.resources.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit with connection:
        testrabbit = TESTHELPERS.get_synchronous_rabbit()
        testrabbit.open_rabbit_connection()

        # Make publish succeed only in 2nd try:
        channel = testrabbit._SynchronousRabbitConnector__channel
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


    @mock.patch('esgfpid.rabbit.synchronous.SynchronousRabbitConnector._SynchronousRabbitConnector__make_connection')
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
            return tests.resources.pikamock.MockPikaBlockingConnection(params)
        connectionmock.side_effect = make_mocked_connection

        # Make test rabbit without connection:
        testrabbit = TESTHELPERS.get_synchronous_rabbit()

        # Run code to be tested:
        testrabbit.send_message_to_queue(msg)

        # Check result:
        # Connection is opened:
        conn = testrabbit._SynchronousRabbitConnector__connection
        self.assertTrue(conn.is_open)
        # Message was sent:
        channel = testrabbit._SynchronousRabbitConnector__channel
        self.assertEquals(channel.success_counter, 1, 'Message should succeed!')
        # Message was sent on first try:
        self.assertEqual(channel.publish_counter, 1,
            'Message was sent %i times!' % channel.publish_counter)
        # Message arrived in mock channel:
        self.assertIn(json.dumps(msg), channel.messages,
            'Message %s not found in channel\'s list: %s' % (msg, channel.messages))
        self.assertIn(key, channel.routing_keys,
            'Routing key %s not found in channel\'s list: %s' % (key, channel.routing_keys))
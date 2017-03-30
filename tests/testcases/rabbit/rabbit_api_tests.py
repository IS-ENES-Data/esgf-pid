import unittest
import mock
import logging
import json
import sys

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

'''
Tests the rabbit module's API (rabbit.py),
using a simple MagicMock for both synchronous and
asynchronous RabbitMQ server connector modules.
'''
class RabbitTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def test_constructor_syn(self,):

        # Test variables:
        args = TESTHELPERS.get_rabbit_args(is_synchronous_mode=True)

        # Run code to be tested:
        testrabbit = esgfpid.rabbit.RabbitMessageSender(**args)

        # Check result
        self.assertIsInstance(testrabbit, esgfpid.rabbit.RabbitMessageSender)
        serverconn = testrabbit._RabbitMessageSender__server_connector
        self.assertIsInstance(serverconn, esgfpid.rabbit.synchronous.SynchronousRabbitConnector,
            'Wrong type: %s' % type(serverconn))
        self.assertFalse(testrabbit._RabbitMessageSender__ASYNCHRONOUS)

    def test_constructor_asyn(self,):

        # Test variables:
        args = TESTHELPERS.get_rabbit_args(is_synchronous_mode=False)
        args['is_synchronous_mode'] = False

        # Run code to be tested:
        testrabbit = esgfpid.rabbit.RabbitMessageSender(**args)

        # Check result
        self.assertIsInstance(testrabbit, esgfpid.rabbit.RabbitMessageSender)
        serverconn = testrabbit._RabbitMessageSender__server_connector
        self.assertIsInstance(serverconn, esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector,
            'Wrong type: %s' % type(serverconn))
        self.assertTrue(testrabbit._RabbitMessageSender__ASYNCHRONOUS)


    def test_open_rabbit_connection(self):

        # Make test rabbit:
        rabbit_syn  = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=True)
        rabbit_asyn = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=False)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_syn)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_asyn)

        # Run code to be tested:
        rabbit_syn.open_rabbit_connection()
        rabbit_asyn.open_rabbit_connection()

        # Check result:
        mock_connector_syn = rabbit_syn._RabbitMessageSender__server_connector
        mock_connector_syn.open_rabbit_connection.assert_called_with()
        mock_connector_asyn = rabbit_asyn._RabbitMessageSender__server_connector
        mock_connector_asyn.open_rabbit_connection.assert_not_called()

    def test_close_rabbit_connection(self):

        # Make test rabbit:
        rabbit_syn  = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=True)
        rabbit_asyn = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=False)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_syn)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_asyn)
        
        # Run code to be tested:
        rabbit_syn.close_rabbit_connection()
        rabbit_asyn.close_rabbit_connection()

        # Check result:
        mock_connector_syn = rabbit_syn._RabbitMessageSender__server_connector
        mock_connector_syn.close_rabbit_connection.assert_called_with()
        mock_connector_asyn = rabbit_asyn._RabbitMessageSender__server_connector
        mock_connector_asyn.close_rabbit_connection.assert_not_called()

    def test_send_message_to_queue(self):

        # Test variables
        msg = 'FOOBAR'

        # Make test rabbit and replace the connector with a mock:
        rabbit_syn  = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=True)
        rabbit_asyn = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=False)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_syn)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_asyn)

        # Run code to be tested
        rabbit_syn.send_message_to_queue(msg)
        rabbit_asyn.send_message_to_queue(msg)

        # Check result
        mock_connector = rabbit_syn._RabbitMessageSender__server_connector
        mock_connector.send_message_to_queue.assert_called_with(msg)
        mock_connector = rabbit_asyn._RabbitMessageSender__server_connector
        mock_connector.send_message_to_queue.assert_called_with(msg)


    def test_is_finished(self):

        # Make test rabbit and replace the connector with a mock:
        rabbit_syn  = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=True)
        rabbit_asyn = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=False)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_syn)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_asyn)

        # Run code to be tested
        res1 = rabbit_syn.is_finished()
        res2 = rabbit_asyn.is_finished()

        # Check result
        self.assertIsNone(res1)
        self.assertTrue(res2, 'We asked the mock to return True, not %s.' % res2)
        mock_connector_asyn = rabbit_asyn._RabbitMessageSender__server_connector
        mock_connector_asyn.is_finished.assert_called()

    def test_finish_ok(self):

        # Make test rabbit and replace the connector with a mock:
        rabbit_syn  = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=True)
        rabbit_asyn = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=False)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_syn)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_asyn)

        # Run code to be tested
        res1 = rabbit_syn.finish()
        res2 = rabbit_asyn.finish()


        # Check result
        mock_connector = rabbit_syn._RabbitMessageSender__server_connector
        mock_connector.finish_rabbit_thread.assert_not_called()
        mock_connector = rabbit_asyn._RabbitMessageSender__server_connector
        mock_connector.finish_rabbit_thread.assert_called_with()


    def test_force_finish_ok(self):

        # Make test rabbit and replace the connector with a mock:
        rabbit_syn  = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=True)
        rabbit_asyn = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=False)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_syn)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_asyn)

        # Run code to be tested
        res1 = rabbit_syn.force_finish()
        res2 = rabbit_asyn.force_finish()

        # Check result
        mock_connector = rabbit_syn._RabbitMessageSender__server_connector
        mock_connector.force_finish_rabbit_thread.assert_not_called()
        mock_connector = rabbit_asyn._RabbitMessageSender__server_connector
        mock_connector.force_finish_rabbit_thread.assert_called_with()


    def test_start_ok(self):

        # Make test rabbit and replace the connector with a mock:
        rabbit_syn  = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=True)
        rabbit_asyn = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=False)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_syn)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_asyn)

        # Run code to be tested
        res1 = rabbit_syn.start()
        res2 = rabbit_asyn.start()

        # Check result
        mock_connector = rabbit_syn._RabbitMessageSender__server_connector
        mock_connector.start_rabbit_thread.assert_not_called()
        mock_connector = rabbit_asyn._RabbitMessageSender__server_connector
        mock_connector.start_rabbit_thread.assert_called_with()


    def test_leftovers_ok(self):
        
        # Make test rabbit and replace the connector with a mock:
        rabbit_asyn = TESTHELPERS.get_rabbit_message_sender(is_synchronous_mode=False)
        TESTHELPERS.patch_rabbit_with_magic_mock(rabbit_asyn)

        # Replace the connector with a mock:
        mock_connector = rabbit_asyn._RabbitMessageSender__server_connector
        mock_connector.any_leftovers.return_value = True
        mock_connector.get_leftovers.return_value = ['a', 'b', 'c']
        
        # Run code to be tested
        result_bool = rabbit_asyn.any_leftovers()
        result_list = rabbit_asyn.get_leftovers()

        # Check result:
        self.assertTrue(result_bool,
            'We asked the mock to return True, not %s.' % result_bool)
        self.assertEquals(result_list, ['a', 'b', 'c'],
            'We asked the mock to return a list, not %s.' % result_list)
        mock_connector.any_leftovers.assert_called_with()
        mock_connector.get_leftovers.assert_called_with()
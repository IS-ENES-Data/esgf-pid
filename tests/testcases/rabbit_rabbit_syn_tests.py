import unittest
import mock
import logging
import json
import sys
sys.path.append("..")
import esgfpid.rabbit

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class RabbitTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')
        self.testrabbit = self.__make_testrabbit_syn()

    def tearDown(self):
        LOGGER.info('#############################')

    def __get_args_dict(self):
        return dict(
            urls_fallback='www.rabbit.foo',
            url_preferred=None,
            exchange_name='exch',
            username='rogerRabbit',
            password='mySecretCarrotDream'
        )

    @mock.patch('esgfpid.defaults')
    def __make_testrabbit_syn(self, defaults_patch):

        # Patch defaults to create a synchronous object:
        defaults_patch.RABBIT_ASYNCHRONOUS = False
        reload(esgfpid.rabbit.rabbit)

        # Create object
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

        # Replace connector with mock:
        testrabbit._RabbitMessageSender__server_connector = mock.MagicMock()

        return testrabbit

    # Tests

    #
    # Synchronous:
    #

    @mock.patch('esgfpid.defaults')
    def test_constructor_mandatory_args_(self, defaults_patch):

        # Preparation:
        defaults_patch.RABBIT_ASYNCHRONOUS = False # <-- This is tested
        reload(esgfpid.rabbit.rabbit)

        # Test variables:
        args = self.__get_args_dict()

        # Run code to be tested:
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

        # Check result
        self.assertIsInstance(testrabbit, esgfpid.rabbit.rabbit.RabbitMessageSender)
        serverconn = testrabbit._RabbitMessageSender__server_connector
        self.assertIsInstance(serverconn, esgfpid.rabbit.synchronous.SynchronousServerConnector,
            'Wrong type: %s' % type(serverconn))

    def test_open_rabbit_connection(self):

        # Make test rabbit:
        testrabbit = self.__make_testrabbit_syn()
        
        # Run code to be tested:
        testrabbit.open_rabbit_connection()

        # Check result:
        mock_connector = testrabbit._RabbitMessageSender__server_connector
        mock_connector.open_rabbit_connection.assert_called_with()

    def test_close_rabbit_connection(self):

        # Make test rabbit:
        testrabbit = self.__make_testrabbit_syn()

        # Replace the connector with a mock:
        testrabbit._RabbitMessageSender__server_connector = mock.MagicMock()
        
        # Run code to be tested:
        testrabbit.close_rabbit_connection()

        # Check result:
        mock_connector = testrabbit._RabbitMessageSender__server_connector
        mock_connector.close_rabbit_connection.assert_called_with()

    def test_send_message_to_queue(self):

        # Test variables
        msg = 'FOOBAR'

        # Make test rabbit and eplace the connector with a mock:
        testrabbit = self.__make_testrabbit_syn()
                
        # Run code to be tested
        testrabbit.send_message_to_queue(msg)

        # Check result
        mock_connector = testrabbit._RabbitMessageSender__server_connector
        mock_connector.send_message_to_queue.assert_called_with(msg)


    def test_is_finished(self):

        # Make test rabbit and eplace the connector with a mock:
        testrabbit = self.__make_testrabbit_syn()
                
        # Run code to be tested
        res = testrabbit.is_finished()

        # Check result
        self.assertIsNone(res)

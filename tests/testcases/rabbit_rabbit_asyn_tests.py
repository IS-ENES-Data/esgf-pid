import unittest
import mock
import logging
import json
import sys
import os
sys.path.append("..")
import esgfpid.rabbit

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class RabbitTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ########## (%s)' % os.path.basename(__file__))

    def tearDown(self):
        LOGGER.info('#############################')

    def __get_args_dict(self):
        return dict(
            urls_fallback='www.rabbit.foo',
            url_preferred=None,
            exchange_name='exch',
            username='rogerRabbit',
            password='mySecretCarrotDream',
            test_publication=False
        )

    def __get_testrabbit_asyn(self):
        args = self.__get_args_dict()
        return esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

    # Tests

    #
    # Asynchronous:
    #

    def test_constructor_mandatory_args(self):

        # Test variables:
        args = self.__get_args_dict()

        # Run code to be tested:
        myrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

        # Check result
        self.assertIsInstance(myrabbit, esgfpid.rabbit.rabbit.RabbitMessageSender)
        serverconn = myrabbit._RabbitMessageSender__server_connector
        self.assertIsInstance(serverconn, esgfpid.rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector,
            'Wrong type: %s' % type(serverconn))

    def test_constructor_adapt_args(self):
        ''' Were the args adapted correctly before being passed on
        to the connector? '''

        # Test variables:
        args = self.__get_args_dict()

        # Run code to be tested:
        myrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

        # Check wether arguments were correctly changed:
        serverconn = myrabbit._RabbitMessageSender__server_connector
        received_args = serverconn._AsynchronousRabbitConnector__args
        self.assertEquals(received_args[ 'exchange_name'], 'exch')
        self.assertEquals(received_args['url_preferred'], 'www.rabbit.foo') # the only fallback url becomes the preferred
        self.assertEquals(received_args['urls_fallback'], []) # no fallback url is left
        self.assertIn('credentials', received_args)
        self.assertNotIn('username', received_args)
        self.assertNotIn('password', received_args)

    def test_finish_ok(self):
        '''
        If we call finish() on the sender, it calls finish_rabbit_thread() on
        the connector, which calls finish_rabbit_thread() on the thread controller.
        '''

        # Prepare
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

        # Replace the connector with a mock:
        testrabbit._RabbitMessageSender__server_connector = mock.MagicMock()
        mock_connector = testrabbit._RabbitMessageSender__server_connector
        
        # Run code to be tested
        testrabbit.finish()

        # Check result:
        mock_connector.finish_rabbit_thread.assert_called_with()

    def test_force_finish_ok(self):
        '''
        If we call force_finish() on the sender, it calls force_finish_rabbit_thread() on
        the connector, which calls force_finish_rabbit_thread() on the thread controller.
        '''

        # Prepare
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

        # Replace the connector with a mock:
        testrabbit._RabbitMessageSender__server_connector = mock.MagicMock()
        mock_connector = testrabbit._RabbitMessageSender__server_connector
        
        # Run code to be tested
        testrabbit.force_finish()

        # Check result:
        mock_connector.force_finish_rabbit_thread.assert_called_with()

    def test_start_ok(self):
        '''
        If we call start() on the sender, it calls start_rabbit_thread() on
        the connector, which calls start_rabbit_thread(**args) on the thread controller.
        '''

        # Prepare
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

        # Replace the connector with a mock:
        testrabbit._RabbitMessageSender__server_connector = mock.MagicMock()
        mock_connector = testrabbit._RabbitMessageSender__server_connector
        
        # Run code to be tested
        testrabbit.start()

        # Check result:
        mock_connector.start_rabbit_thread.assert_called_with()


    def test_is_finished_ok(self):
        '''
        If we call is_finished() on the sender, it calls is_finished() on
        the connector, which calls is_finished() on the thread controller.
        '''

        # Prepare
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

        # Replace the connector with a mock:
        testrabbit._RabbitMessageSender__server_connector = mock.MagicMock()
        mock_connector = testrabbit._RabbitMessageSender__server_connector
                
        # Run code to be tested
        res = testrabbit.is_finished()

        # Check result:
        self.assertTrue(res, 'We asked the mock to return True, not %s.' % res)
        mock_connector.is_finished.assert_called_with()

    def test_leftovers_ok(self):
        '''
        If we call get_leftovers() on the sender, it calls get_leftovers() on
        the connector, which calls get_leftovers() on the thread controller.
        '''
        # Prepare
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

        # Replace the connector with a mock:
        testrabbit._RabbitMessageSender__server_connector = mock.MagicMock()
        mock_connector = testrabbit._RabbitMessageSender__server_connector
        mock_connector.any_leftovers.return_value = True
        mock_connector.get_leftovers.return_value = ['a', 'b', 'c']
        
        # Run code to be tested
        result_bool = testrabbit.any_leftovers()
        result_list = testrabbit.get_leftovers()

        # Check result:
        self.assertTrue(result_bool,
            'We asked the mock to return True, not %s.' % result_bool)
        self.assertEquals(result_list, ['a', 'b', 'c'],
            'We asked the mock to return a list, not %s.' % result_list)
        mock_connector.any_leftovers.assert_called_with()
        mock_connector.get_leftovers.assert_called_with()

    def test_send_message_to_queue(self):

        # Prepare
        msg = 'FOOBAR'
        args = self.__get_args_dict()
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

        # Replace the connector with a mock:
        testrabbit._RabbitMessageSender__server_connector = mock.MagicMock()
        mock_connector = testrabbit._RabbitMessageSender__server_connector
                
        # Run code to be tested
        testrabbit.send_message_to_queue(msg)

        # Check result
        mock_connector.send_message_to_queue.assert_called_with(msg)

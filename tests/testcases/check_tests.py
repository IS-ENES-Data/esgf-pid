import unittest
import mock
import logging
import pika.exceptions as pikaexceptions
import tests.utils.captureconsoleoutput
import tests.mocks.pikamock
import tests.resources.error_messages
import esgfpid.utils
import esgfpid.check
import pika

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

import globalvar
if globalvar.QUICK_ONLY:
    print('Skipping slow tests in module "%s".' % __name__)


class CheckTestCase(unittest.TestCase):

    slow_message = '\nRunning a slow test (avoid by using -ls flag).'

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    #
    # Init
    #

    def test_init_rabbitchecker_no_preferred(self):
        ''' Test if the constructor of the RabbitChecker works fine (with foo bar values).'''

        # Test variables:
        rabbit1 = dict(
            user = 'johndoe',
            password = 'abc123yx',
            url = 'this.is.my.only.host')
        testconnector = TESTHELPERS.get_connector(messaging_service_credentials=[rabbit1])

        # Run code to be tested:
        testobject = esgfpid.check.RabbitChecker(
            connector = testconnector,
            print_to_console = True,
            print_success_to_console = True
        )

        # Check result:
        self.assertIsInstance(testobject, esgfpid.check.RabbitChecker,
            'Init with placeholder params did not work.')

    def test_init_rabbitchecker_missing_args(self):
        ''' Test if the constructor of the RabbitChecker complains if an argument is missing.'''

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            testobject = esgfpid.check.RabbitChecker(
                print_to_console = True,
                print_success_to_console = True
            )

    #
    # Positive check
    #

    @mock.patch('esgfpid.check.RabbitChecker._RabbitChecker__pika_blocking_connection')
    def test_run_check_ok(self, connection_patch):
        ''' This checks if the entire function works fine in case
        the rabbit is reacked (connection and channel ok), and if it prints 
        the correct message.
        '''

        # Define the replacement for the patched method:
        mock_response = tests.mocks.pikamock.MockPikaBlockingConnection(mock.MagicMock())
        connection_patch.return_value = mock_response

        # Test variables:
        rabbit1 = dict(
            user = 'johndoe',
            password = 'abc123yx',
            url = 'this.is.my.only.host')
        testconnector = TESTHELPERS.get_connector(messaging_service_credentials=[rabbit1])


        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                connector = testconnector,
                print_to_console = True,
                print_success_to_console = True
            )

        # Expected message:
        expected_message = tests.resources.error_messages.expected_message_ok

        # Check result:
        output = out.getvalue().strip()
        self.assertEquals(expected_message, output,
            'Wrong error message.\n\nWe expected:\n\n'+expected_message+'\n\nWe got:\n\n'+output+'\n')

    #
    # Connection failures
    #

    '''
    Test if the correct error message gets printed by the check method on connection failure.
    Connection failure does not need to be mocked. We really try to connect here.
    '''
    @unittest.skipIf(globalvar.QUICK_ONLY, '(this test is slow)')
    def test_run_check_connection_failed(self):

        print(self.slow_message)

        # Test variables:
        rabbit1 = dict(
            user = 'johndoe',
            password = 'abc123yx',
            url = 'this.is.my.only.host')
        testconnector = TESTHELPERS.get_connector(messaging_service_credentials=[rabbit1])

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                connector = testconnector,
                print_to_console = True,
                print_success_to_console = True
            )

        # Expected message:
        expected_message = tests.resources.error_messages.expected_message_connection_failed_only

        # Check result:
        output = out.getvalue().strip()
        self.assertEquals(expected_message, output,
            'Wrong error message.\n\nWe expected:\n\n'+expected_message+'\n\nWe got:\n\n'+output+'\n')

    @mock.patch('esgfpid.check.RabbitChecker._RabbitChecker__pika_blocking_connection')
    def test_run_check_connection_failed_unknown(self, connection_patch):
        '''
        Test if the correct error message gets printed by the check method on connection failure.
        Connection failure does not need to be mocked. We really try to connect here.
        '''

        # Define the replacement for the patched method
        connection_patch.return_value = None

        # Test variables:
        rabbit1 = dict(
            user = 'johndoe',
            password = 'abc123yx',
            url = 'this.is.my.only.host')
        testconnector = TESTHELPERS.get_connector(messaging_service_credentials=[rabbit1])

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                connector = testconnector,
                print_to_console = True,
                print_success_to_console = True
            )

        # Expected message:
        expected_message = tests.resources.error_messages.expected_message_connection_failed_unknown
        
        # Check result:
        output = out.getvalue().strip()
        self.assertEquals(expected_message, output,
            'Wrong error message.\n\nWe expected:\n\n'+expected_message+'\n\nWe got:\n\n'+output+'\n')


    '''Test if the correct error message gets printed by the check method on connection failure.'''
    @unittest.skipIf(globalvar.QUICK_ONLY, '(this test is slow)')
    def test_run_check_connection_failed_several_urls(self):

        print(self.slow_message)

        # Test variables:
        #messaging_service_urls = ['tomato.salad-with-spam.fr', 'mystery-tour.uk']
        #messaging_service_url_preferred = 'this.is.my.favourite.host'
        user = 'johndoe'
        pw = 'abc123yx'
        rabbit1 = dict(
            user = user,
            password = pw,
            url = 'this.is.my.favourite.host',
            priority=1)
        rabbit2 = dict(
            user = user,
            password = pw,
            priority=2,
            url = 'mystery-tour.uk')
        rabbit3 = dict(
            user = user,
            password = pw,
            priority=3,
            url = 'tomato.salad-with-spam.fr')
        testconnector = TESTHELPERS.get_connector(messaging_service_credentials=[rabbit1,rabbit2,rabbit3])

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                connector = testconnector,
                print_to_console = True,
                print_success_to_console = True
            )

        # Expected message:
        expected_message = tests.resources.error_messages.expected_message_connection_failed_several

        # Check result:
        output = out.getvalue().strip()
        self.assertEquals(expected_message, output,
            'Wrong error message.\n\nWe expected:\n\n'+expected_message+'\n\nWe got:\n\n'+output+'\n')

    @mock.patch('esgfpid.check.RabbitChecker._RabbitChecker__pika_blocking_connection')
    def test_run_check_first_connection_failed(self, connection_patch):
        '''
        Test if the correct error message gets printed by the check method on connection failure.
        Connection failure does not need to be mocked. We really try to connect here.
        '''

        # Test variables:
        #messaging_service_url_preferred = 'this.is.my.favourite.host'
        #messaging_service_urls =  ['tomato.salad-with-spam.fr', 'mystery-tour.uk']
        user = 'johndoe'
        pw = 'abc123yx'
        rabbit1 = dict(
            user = user,
            password = pw,
            priority=1,
            url = 'this.is.my.favourite.host')
        rabbit2 = dict(
            user = user,
            password = pw,
            priority=3,
            url = 'tomato.salad-with-spam.fr')
        rabbit3 = dict(
            user = user,
            password = pw,
            priority=2,
            url = 'mystery-tour.uk')
        testconnector = TESTHELPERS.get_connector(messaging_service_credentials=[rabbit1,rabbit2,rabbit3])

        # Define the replacement for the patched method:
        def different_mock_response_depending_on_host(params):
            if params.host == 'tomato.salad-with-spam.fr':
                return tests.mocks.pikamock.MockPikaBlockingConnection(mock.MagicMock())
            else:
                raise pika.exceptions.ConnectionClosed      
        connection_patch.side_effect = different_mock_response_depending_on_host

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                connector = testconnector,
                print_to_console = True,
                print_success_to_console = True
            )

        # Expected message:
        expected_message = tests.resources.error_messages.expected_message_first_connection_failed_then_ok

        # Check result:
        output = out.getvalue().strip()
        self.assertEquals(expected_message, output,
            'Wrong error message.\n\nWe expected:\n\n'+expected_message+'\n\nWe got:\n\n'+output+'\n')



    #
    # Authentication failure
    #

    @mock.patch('esgfpid.check.RabbitChecker._RabbitChecker__pika_blocking_connection')
    def test_run_check_auth_error(self, connection_patch):

        # Define the replacement for the patched method:
        mock_response = tests.mocks.pikamock.MockPikaBlockingConnection(mock.MagicMock())
        connection_patch.return_value = mock_response
        connection_patch.side_effect = pikaexceptions.ProbableAuthenticationError

        # Test variables:
        rabbit1 = dict(
            user = 'johndoe',
            password = 'abc123yx',
            url = 'this.is.my.only.host')
        testconnector = TESTHELPERS.get_connector(messaging_service_credentials=[rabbit1])

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                connector = testconnector,
                print_to_console = True,
                print_success_to_console = True
            )

        # Expected message:
        expected_message = tests.resources.error_messages.expected_message_authentication_failed_only

        # Check result:
        output = out.getvalue().strip()
        self.assertEquals(expected_message, output,
            'Wrong error message.\n\nWe expected:\n\n'+expected_message+'\n\nWe got:\n\n'+output+'\n')


    @mock.patch('esgfpid.check.RabbitChecker._RabbitChecker__pika_blocking_connection')
    def test_run_check_auth_error_three_urls(self, connection_patch):

        # Define the replacement for the patched method:
        mock_response = tests.mocks.pikamock.MockPikaBlockingConnection(mock.MagicMock())
        connection_patch.return_value = mock_response
        connection_patch.side_effect = pikaexceptions.ProbableAuthenticationError

        # Test variables:
        #messaging_service_urls =  ['tomato.salad-with-spam.fr', 'mystery-tour.uk']
        #messaging_service_url_preferred = 'this.is.my.favourite.host'
        user = 'johndoe'
        pw = 'abc123yx'
        rabbit1 = dict(
            user = user,
            password = pw,
            url = 'this.is.my.favourite.host',
            priority=1)
        rabbit2 = dict(
            user = user,
            password = pw,
            priority=3,
            url = 'tomato.salad-with-spam.fr')
        rabbit3 = dict(
            user = user,
            password = pw,
            priority=2,
            url = 'mystery-tour.uk')
        testconnector = TESTHELPERS.get_connector(messaging_service_credentials=[rabbit1,rabbit2,rabbit3])

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                connector = testconnector,
                print_to_console = True,
                print_success_to_console = True
            )

        # Expected message:
        expected_message = tests.resources.error_messages.expected_message_authentication_failed_several

        # Check result:
        output = out.getvalue().strip()
        self.assertEquals(expected_message, output,
            'Wrong error message.\n\nWe expected:\n\n'+expected_message+'\n\nWe got:\n\n'+output+'\n')

    #
    # Channel error
    #

    @mock.patch('esgfpid.check.RabbitChecker._RabbitChecker__pika_blocking_connection')
    def test_run_check_channel_error(self, connection_patch):

        # Define the replacement for the patched method:
        mock_response = tests.mocks.pikamock.MockPikaBlockingConnection(mock.MagicMock())
        mock_response.raise_channel_closed = True
        connection_patch.return_value = mock_response

        # Test variables:
        rabbit1 = dict(
            user = 'johndoe',
            password = 'abc123yx',
            url = 'this.is.my.only.host')
        testconnector = TESTHELPERS.get_connector(messaging_service_credentials=[rabbit1])

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                connector = testconnector,
                print_to_console = True,
                print_success_to_console = True
            )

        # Expected message:
        expected_message = tests.resources.error_messages.expected_message_channel_closed_only

        # Check result:
        output = out.getvalue().strip()
        self.assertEquals(expected_message, output,
            'Wrong error message.\n\nWe expected:\n\n'+expected_message+'\n\nWe got:\n\n'+output+'\n')

    @mock.patch('esgfpid.check.RabbitChecker._RabbitChecker__pika_blocking_connection')
    def test_run_check_channel_error_first(self, connection_patch):
        '''
        Test if the correct error message gets printed by the check method on connection failure.
        Connection failure does not need to be mocked. We really try to connect here.
        '''

        # Test variables:
        #messaging_service_url_preferred = 'this.is.my.favourite.host'
        #messaging_service_urls =  ['tomato.salad-with-spam.fr', 'mystery-tour.uk']
        user = 'johndoe'
        pw = 'abc123yx'
        rabbit1 = dict(
            user = user,
            password = pw,
            priority=1,
            url = 'this.is.my.favourite.host')
        rabbit2 = dict(
            user = user,
            password = pw,
            priority=3,
            url = 'tomato.salad-with-spam.fr')
        rabbit3 = dict(
            user = user,
            password = pw,
            priority=2,
            url = 'mystery-tour.uk')
        testconnector = TESTHELPERS.get_connector(messaging_service_credentials=[rabbit1,rabbit2,rabbit3])

        # Define the replacement for the patched method:
        def different_mock_response_depending_on_host(params):
            conn = tests.mocks.pikamock.MockPikaBlockingConnection(mock.MagicMock())
            if params.host == 'tomato.salad-with-spam.fr':
                conn.raise_channel_closed = False
            else:
                conn.raise_channel_closed = True
            return conn
        connection_patch.side_effect = different_mock_response_depending_on_host

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                connector = testconnector,
                print_to_console = True,
                print_success_to_console = True
            )

        # Expected message:
        expected_message = tests.resources.error_messages.expected_message_first_channel_closed_then_ok

        # Check result:
        output = out.getvalue().strip()
        self.assertEquals(expected_message, output,
            'Wrong error message.\n\nWe expected:\n\n'+expected_message+'\n\nWe got:\n\n'+output+'\n')

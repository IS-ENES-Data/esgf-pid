import sys
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


class CheckTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')

    def tearDown(self):
        LOGGER.info('#############################')

    #
    # Init
    #

    def test_init_rabbitchecker_no_preferred(self):
        ''' Test if the constructor of the RabbitChecker works fine (with foo bar values).'''

        # Test variables:
        messaging_service_urls = 'this.is.my.only.host'
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

        # Run code to be tested:
        testobject = esgfpid.check.RabbitChecker(
            messaging_service_urls = messaging_service_urls,
            messaging_service_password = messaging_service_password,
            print_to_console = print_to_console,
            messaging_service_username = messaging_service_username,
            print_success_to_console = True
        )

        # Check result:
        self.assertIsInstance(testobject, esgfpid.check.RabbitChecker,
            'Init with placeholder params did not work.')
        self.assertIn('this.is.my.only.host', testobject._RabbitChecker__current_rabbit_host)

    def test_init_rabbitchecker_only_preferred(self):
        ''' Test if the constructor of the RabbitChecker works fine (with foo bar values).'''

        # Test variables:
        url = 'this.is.my.favourite.host'
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

        # Run code to be tested:
        testobject = esgfpid.check.RabbitChecker(
            messaging_service_url_preferred = url,
            messaging_service_password = messaging_service_password,
            print_to_console = print_to_console,
            messaging_service_username = messaging_service_username,
            print_success_to_console = True
        )

        # Check result:
        self.assertIsInstance(testobject, esgfpid.check.RabbitChecker,
            'Init with placeholder params did not work.')
        self.assertIn(url, testobject._RabbitChecker__current_rabbit_host)

    def test_init_rabbitchecker_preferred_and_fallback(self):
        ''' Test if the constructor of the RabbitChecker works fine (with foo bar values).'''

        # Test variables:
        fave = 'this.is.my.favourite.host'
        urls = ['tomato.salad-with-spam.fr', 'magical-mystery-tour.uk']
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

        # Run code to be tested:
        testobject = esgfpid.check.RabbitChecker(
            messaging_service_urls = urls,
            messaging_service_url_preferred = fave,
            messaging_service_password = messaging_service_password,
            print_to_console = print_to_console,
            messaging_service_username = messaging_service_username,
            print_success_to_console = True
        )

        # Check result:
        self.assertIsInstance(testobject, esgfpid.check.RabbitChecker,
            'Init with placeholder params did not work.')
        self.assertIn(fave, testobject._RabbitChecker__current_rabbit_host)
        self.assertIn(urls[0], testobject._RabbitChecker__rabbit_hosts)
        self.assertIn(urls[1], testobject._RabbitChecker__rabbit_hosts)

    def test_init_rabbitchecker_missing_args(self):
        ''' Test if the constructor of the RabbitChecker complains if an argument is missing.'''

        # Test variables:
        messaging_service_urls = 'this.is.my.only.host'
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = False

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            testobject = esgfpid.check.RabbitChecker(
                messaging_service_urls = messaging_service_urls,
                print_to_console = print_to_console,
                messaging_service_username = messaging_service_username,
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
        messaging_service_urls = 'this.is.my.only.host'
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                messaging_service_urls = messaging_service_urls,
                messaging_service_password = messaging_service_password,
                print_to_console = print_to_console,
                messaging_service_username = messaging_service_username,
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

    def test_run_check_connection_failed(self):
        '''
        Test if the correct error message gets printed by the check method on connection failure.
        Connection failure does not need to be mocked. We really try to connect here.
        '''

        # Test variables:
        messaging_service_urls = 'this.is.my.only.host'
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                messaging_service_urls = messaging_service_urls,
                messaging_service_password = messaging_service_password,
                print_to_console = print_to_console,
                messaging_service_username = messaging_service_username,
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
        messaging_service_urls = 'this.is.my.only.host'
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                messaging_service_urls = messaging_service_urls,
                messaging_service_password = messaging_service_password,
                print_to_console = print_to_console,
                messaging_service_username = messaging_service_username,
                print_success_to_console = True
            )

        # Expected message:
        expected_message = tests.resources.error_messages.expected_message_connection_failed_unknown
        
        # Check result:
        output = out.getvalue().strip()
        self.assertEquals(expected_message, output,
            'Wrong error message.\n\nWe expected:\n\n'+expected_message+'\n\nWe got:\n\n'+output+'\n')

    def test_run_check_connection_failed_several_urls(self):
        '''Test if the correct error message gets printed by the check method on connection failure.'''

        # Test variables:
        messaging_service_urls = ['tomato.salad-with-spam.fr', 'mystery-tour.uk']
        messaging_service_url_preferred = 'this.is.my.favourite.host'
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                messaging_service_urls = messaging_service_urls,
                messaging_service_url_preferred = messaging_service_url_preferred,
                messaging_service_password = messaging_service_password,
                print_to_console = print_to_console,
                messaging_service_username = messaging_service_username,
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
        messaging_service_url_preferred = 'this.is.my.favourite.host'
        messaging_service_urls =  ['tomato.salad-with-spam.fr', 'mystery-tour.uk']
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

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
                messaging_service_url_preferred=messaging_service_url_preferred,
                messaging_service_urls = messaging_service_urls,
                messaging_service_password = messaging_service_password,
                print_to_console = print_to_console,
                messaging_service_username = messaging_service_username,
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
        messaging_service_urls = 'this.is.my.only.host'
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                messaging_service_urls = messaging_service_urls,
                messaging_service_password = messaging_service_password,
                print_to_console = print_to_console,
                messaging_service_username = messaging_service_username,
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
        messaging_service_urls =  ['tomato.salad-with-spam.fr', 'mystery-tour.uk']
        messaging_service_url_preferred = 'this.is.my.favourite.host'
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                messaging_service_urls = messaging_service_urls,
                messaging_service_url_preferred = messaging_service_url_preferred,
                messaging_service_password = messaging_service_password,
                print_to_console = print_to_console,
                messaging_service_username = messaging_service_username,
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
        messaging_service_urls = 'this.is.my.only.host'
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

        # Run code to be tested and capture stout:
        with tests.utils.captureconsoleoutput.captured_output() as (out, err):
            esgfpid.check.check_pid_queue_availability(
                messaging_service_urls = messaging_service_urls,
                messaging_service_password = messaging_service_password,
                print_to_console = print_to_console,
                messaging_service_username = messaging_service_username,
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
        messaging_service_url_preferred = 'this.is.my.favourite.host'
        messaging_service_urls =  ['tomato.salad-with-spam.fr', 'mystery-tour.uk']
        messaging_service_username = 'johndoe'
        messaging_service_password = 'abc123yx'
        print_to_console = True

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
                messaging_service_url_preferred=messaging_service_url_preferred,
                messaging_service_urls = messaging_service_urls,
                messaging_service_password = messaging_service_password,
                print_to_console = print_to_console,
                messaging_service_username = messaging_service_username,
                print_success_to_console = True
            )

        # Expected message:
        expected_message = tests.resources.error_messages.expected_message_first_channel_closed_then_ok

        # Check result:
        output = out.getvalue().strip()
        self.assertEquals(expected_message, output,
            'Wrong error message.\n\nWe expected:\n\n'+expected_message+'\n\nWe got:\n\n'+output+'\n')

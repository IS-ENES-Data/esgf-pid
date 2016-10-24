import unittest
import mock
import logging
import copy
import os
import esgfpid.rabbit.asynchronous.thread_acceptor
from esgfpid.rabbit.asynchronous.exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class ThreadAcceptorTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ########## (%s)' % os.path.basename(__file__))

    def tearDown(self):
        LOGGER.info('#############################')

    def make_acceptor(self):
        self.thread = mock.MagicMock()
        self.feeder = mock.MagicMock()
        self.statemachine = esgfpid.rabbit.asynchronous.thread_statemachine.StateMachine()
        acceptor = esgfpid.rabbit.asynchronous.thread_acceptor.PublicationReceiver(
            self.thread,
            self.statemachine,
            self.feeder)
        self.statemachine._StateMachine__state = self.statemachine._StateMachine__IS_AVAILABLE
        self.thread._connection = mock.MagicMock()
        return acceptor

    # Tests

    #
    # Sending messages
    #

    def test_send_message_ok(self):

        # Preparation:
        acceptor = self.make_acceptor()

        # Run code to be tested:
        acceptor.send_a_message('foo')

        # Check result:
        self.feeder.put_message_into_queue_of_unsent_messages.assert_called_with('foo')
        self.thread._connection.add_timeout.assert_called_with(0, self.feeder.publish_message)


    def test_send_many_messages_ok(self):

        # Preparation:
        acceptor = self.make_acceptor()

        # Run code to be tested:
        acceptor.send_many_messages(['foo','bar'])

        # Check result:
        self.feeder.put_message_into_queue_of_unsent_messages.assert_any_call('bar')
        self.feeder.put_message_into_queue_of_unsent_messages.assert_any_call('foo')
        self.thread._connection.add_timeout.call_count = 2

    def test_resend_message_ok(self):

        # Preparation:
        acceptor = self.make_acceptor()
        channel = mock.MagicMock()
        self.thread._connection._channel = channel
        message = '{"foo":"bar","ROUTING_KEY":"myfoo"}'

        # Run code to be tested:

        acceptor.on_message_not_accepted(channel, 'serverreturnfoo', 'propsfoo', message)

        # Check result:
        exp = '{"original_routing_key": "myfoo", "foo": "bar", "ROUTING_KEY": "cmip6.publisher.HASH.emergency"}'
        self.feeder.put_message_into_queue_of_unsent_messages.assert_called_with(exp)
        self.thread._connection.add_timeout.assert_called_with(0, self.feeder.publish_message)

    def test_send_message_no_more_accept(self):

        # Preparation:
        acceptor = self.make_acceptor()
        self.statemachine._StateMachine__state = self.statemachine._StateMachine__IS_AVAILABLE_BUT_WANTS_TO_STOP

        # Run code to be tested:
        with self.assertRaises(OperationNotAllowed) as e:
            acceptor.send_a_message('foo')
        self.assertIn('Accepting no more messages', e.exception.message)
        with self.assertRaises(OperationNotAllowed) as e:
            acceptor.send_many_messages(['foo','bar'])
        self.assertIn('Accepting no more messages', e.exception.message)

    def test_send_message_cannot_trigger_1(self):

        # Preparation:
        acceptor = self.make_acceptor()
        self.statemachine._StateMachine__state = self.statemachine._StateMachine__WAITING_TO_BE_AVAILABLE
        
        # Run code to be tested:
        acceptor.send_a_message('foo')

        # Check result:
        self.feeder.put_message_into_queue_of_unsent_messages.assert_called_with('foo')
        self.thread._connection.add_timeout.assert_not_called()

    def test_send_message_cannot_trigger_2(self):

        # Preparation:
        acceptor = self.make_acceptor()
        self.statemachine._StateMachine__state = self.statemachine._StateMachine__NOT_STARTED_YET

        # Run code to be tested:
        acceptor.send_a_message('foo')

        # Check result:
        self.feeder.put_message_into_queue_of_unsent_messages.assert_called_with('foo')
        self.thread._connection.add_timeout.assert_not_called()

    def test_send_message_cannot_trigger_3(self):

        # Preparation:
        acceptor = self.make_acceptor()
        self.statemachine._StateMachine__state = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.statemachine.could_not_connect = True

        # Run code to be tested:
        acceptor.send_a_message('foo')

        # Check result:
        self.feeder.put_message_into_queue_of_unsent_messages.assert_called_with('foo')
        self.thread._connection.add_timeout.assert_not_called()

    def test_send_message_cannot_trigger_4(self):

        # Preparation:
        acceptor = self.make_acceptor()
        self.statemachine._StateMachine__state = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.statemachine.could_not_connect = False
        self.statemachine.closed_by_publisher = True

        # Run code to be tested:
        acceptor.send_many_messages(['foo','bar'])

        # Check result:
        self.feeder.put_message_into_queue_of_unsent_messages.assert_any_call('bar')
        self.feeder.put_message_into_queue_of_unsent_messages.assert_any_call('foo')
        self.thread._connection.add_timeout.assert_not_called()

import unittest
import mock
import logging
import copy
import os
import esgfpid.rabbit.asynchronous.thread_feeder
from esgfpid.rabbit.asynchronous.exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

class ThreadFeederTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def make_feeder(self):

        thread = TESTHELPERS.get_thread_mock()
        statemachine = esgfpid.rabbit.asynchronous.thread_statemachine.StateMachine()
        nodemanager = TESTHELPERS.get_nodemanager()

        feeder = esgfpid.rabbit.asynchronous.thread_feeder.RabbitFeeder(
            thread,
            statemachine,
            nodemanager)

        statemachine.set_to_available()
        return feeder, thread

    # Tests

    #
    # Sending messages
    #

    def test_send_message_ok(self):

        # Preparation:
        msg = "{'foo':'bar'}"
        feeder, thread = self.make_feeder()
        thread.messages.append(msg)

        # Run code to be tested:
        feeder.publish_message()

        # Check result:
        #feeder.put_message_into_queue_of_unsent_messages.assert_called_with('foo')
        thread._channel.basic_publish.assert_any_call()
        self.assertNotIn(msg, thread.messages)


    if False:

        def test_send_many_messages_ok(self):

            # Preparation:
            feeder = self.make_feeder()

            # Run code to be tested:
            feeder.send_many_messages(['foo','bar'])

            # Check result:
            self.feeder.put_message_into_queue_of_unsent_messages.assert_any_call('bar')
            self.feeder.put_message_into_queue_of_unsent_messages.assert_any_call('foo')
            self.thread._connection.add_timeout.call_count = 2

        def test_resend_message_ok(self):

            # Preparation:
            feeder = self.make_feeder()
            channel = mock.MagicMock()
            self.thread._connection._channel = channel
            message = '{"foo":"bar","ROUTING_KEY":"myfoo"}'

            # Run code to be tested:

            feeder.on_message_not_accepted(channel, 'serverreturnfoo', 'propsfoo', message)

            # Check result:
            exp = '{"original_routing_key": "myfoo", "foo": "bar", "ROUTING_KEY": "cmip6.publisher.HASH.emergency"}'
            self.feeder.put_message_into_queue_of_unsent_messages.assert_called_with(exp)
            self.thread._connection.add_timeout.assert_called_with(0, self.feeder.publish_message)

        def test_send_message_no_more_accept(self):

            # Preparation:
            feeder = self.make_feeder()
            self.statemachine._StateMachine__state = self.statemachine._StateMachine__IS_AVAILABLE_BUT_WANTS_TO_STOP

            # Run code to be tested:
            with self.assertRaises(OperationNotAllowed) as e:
                feeder.publish_message('foo')
            self.assertIn('Accepting no more messages', e.exception.message)
            with self.assertRaises(OperationNotAllowed) as e:
                feeder.send_many_messages(['foo','bar'])
            self.assertIn('Accepting no more messages', e.exception.message)

        def test_send_message_cannot_trigger_1(self):

            # Preparation:
            feeder = self.make_feeder()
            self.statemachine._StateMachine__state = self.statemachine._StateMachine__WAITING_TO_BE_AVAILABLE
            
            # Run code to be tested:
            feeder.publish_message('foo')

            # Check result:
            self.feeder.put_message_into_queue_of_unsent_messages.assert_called_with('foo')
            self.thread._connection.add_timeout.assert_not_called()

        def test_send_message_cannot_trigger_2(self):

            # Preparation:
            feeder = self.make_feeder()
            self.statemachine._StateMachine__state = self.statemachine._StateMachine__NOT_STARTED_YET

            # Run code to be tested:
            feeder.publish_message('foo')

            # Check result:
            self.feeder.put_message_into_queue_of_unsent_messages.assert_called_with('foo')
            self.thread._connection.add_timeout.assert_not_called()

        def test_send_message_cannot_trigger_3(self):

            # Preparation:
            feeder = self.make_feeder()
            self.statemachine._StateMachine__state = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
            self.statemachine.could_not_connect = True

            # Run code to be tested:
            feeder.publish_message('foo')

            # Check result:
            self.feeder.put_message_into_queue_of_unsent_messages.assert_called_with('foo')
            self.thread._connection.add_timeout.assert_not_called()

        def test_send_message_cannot_trigger_4(self):

            # Preparation:
            feeder = self.make_feeder()
            self.statemachine._StateMachine__state = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
            self.statemachine.could_not_connect = False
            self.statemachine.closed_by_publisher = True

            # Run code to be tested:
            feeder.send_many_messages(['foo','bar'])

            # Check result:
            self.feeder.put_message_into_queue_of_unsent_messages.assert_any_call('bar')
            self.feeder.put_message_into_queue_of_unsent_messages.assert_any_call('foo')
            self.thread._connection.add_timeout.assert_not_called()

import unittest
import logging
import Queue
import pika
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

    def make_feeder(self, error=None):

        thread = TESTHELPERS.get_thread_mock2(error)
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
        # Publish was called:
        thread._channel.basic_publish.assert_called_once()
        # Message not waiting in queue anymore:
        self.assertNotIn(msg, thread.messages)


    def test_send_message_empty(self):

        # Preparation:
        feeder, thread = self.make_feeder()

        # Run code to be tested:
        feeder.publish_message()
        feeder.publish_message()

        # Check result:
        # Publish was not called:
        thread._channel.basic_publish.assert_not_called()
  

    def test_send_message_error(self):

        # Preparation:
        msg = "{'foo':'bar'}"
        feeder, thread = self.make_feeder(error=pika.exceptions.ChannelClosed)
        thread.messages.append(msg)

        # Run code to be tested:
        feeder.publish_message()

        # Check result:
        # Publish was tried:
        thread._channel.basic_publish.assert_called_once()
        # Message was put back to queue:
        self.assertIn(msg, thread.messages)
        self.assertIn(msg, thread.put_back)

    def test_send_message_NOT_STARTED_YET(self):

        # Preparation:
        msg = "{'foo':'bar'}"
        feeder, thread = self.make_feeder()
        thread.messages.append(msg)
        feeder.statemachine._StateMachine__state = 0
        
        # Run code to be tested:
        feeder.publish_message()

        # Check result:
        self.assertTrue(feeder.statemachine.is_NOT_STARTED_YET())
        self.assertIn(msg, thread.messages)
        thread._channel.basic_publish.assert_not_called()

    def test_send_message_PERMANENTLY_UNAVAILABLE_1(self):

        # Preparation:
        msg = "{'foo':'bar'}"
        feeder, thread = self.make_feeder()
        thread.messages.append(msg)
        feeder.statemachine.set_to_permanently_unavailable()
        feeder.statemachine.set_detail_closed_by_publisher()

        # Run code to be tested:
        feeder.publish_message()

        # Check result:
        self.assertTrue(feeder.statemachine.is_PERMANENTLY_UNAVAILABLE())
        self.assertIn(msg, thread.messages)
        thread._channel.basic_publish.assert_not_called()

    def test_send_message_PERMANENTLY_UNAVAILABLE_2(self):

        # Preparation:
        msg = "{'foo':'bar'}"
        feeder, thread = self.make_feeder()
        thread.messages.append(msg)
        feeder.statemachine.set_to_permanently_unavailable()
        feeder.statemachine.detail_could_not_connect = True

        # Run code to be tested:
        feeder.publish_message()

        # Check result:
        self.assertTrue(feeder.statemachine.is_PERMANENTLY_UNAVAILABLE())
        self.assertIn(msg, thread.messages)
        thread._channel.basic_publish.assert_not_called()

    def test_send_message_WAITING_TO_BE_AVAILABLE(self):

        # Preparation:
        msg = "{'foo':'bar'}"
        feeder, thread = self.make_feeder()
        thread.messages.append(msg)
        feeder.statemachine.set_to_waiting_to_be_available()
        
        # Run code to be tested:
        feeder.publish_message()

        # Check result:
        self.assertIn(msg, thread.messages)
        self.assertTrue(feeder.statemachine.is_WAITING_TO_BE_AVAILABLE())
        thread._channel.basic_publish.assert_not_called()

    if False:

        # is other module
        def test_resend_message_ok(self):

            # Preparation:
            msg = '{"foo":"bar","ROUTING_KEY":"myfoo"}'
            feeder, thread = self.make_feeder()
            thread.messages.append(msg)
            #channel = mock.MagicMock()
            #self.thread._connection._channel = channel

            # Run code to be tested:

            feeder.on_message_not_accepted(channel, 'serverreturnfoo', 'propsfoo', message)

            # Check result:
            exp = '{"original_routing_key": "myfoo", "foo": "bar", "ROUTING_KEY": "cmip6.publisher.HASH.emergency"}'
            self.feeder.put_message_into_queue_of_unsent_messages.assert_called_with(exp)
            self.thread._connection.add_timeout.assert_called_with(0, self.feeder.publish_message)

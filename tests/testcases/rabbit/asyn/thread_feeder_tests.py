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

    def test_reset_delivery_number(self):

        # Preparation:
        msg = "{'foo':'bar'}"
        feeder, thread = self.make_feeder()
        thread.messages.append(msg)
        thread.messages.append(msg)
        thread.messages.append(msg)

        # Pre-Check:
        self.assertEquals(feeder._RabbitFeeder__delivery_number, 1)

        # Increase delivery number:
        feeder.publish_message()
        feeder.publish_message()
        feeder.publish_message()
        thread._channel.basic_publish.assert_called()

        # Check if it was increased:
        self.assertEquals(feeder._RabbitFeeder__delivery_number, 4)

        # Run code to be tested:
        feeder.reset_delivery_number()

        # Check if it was reset:
        self.assertEquals(feeder._RabbitFeeder__delivery_number, 1)


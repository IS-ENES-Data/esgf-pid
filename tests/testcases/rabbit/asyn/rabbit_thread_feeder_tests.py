import unittest
import mock
import logging
import copy
import os
import esgfpid.rabbit.asynchronous.thread_feeder
from esgfpid.rabbit.asynchronous.exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

UNCONFIRMED_TAGS = [1,2,3,4]
UNCONFIRMED_MESSAGES = {'1':'foo1', '2':'foo2', '3':'foo3', '4':'foo4'}


class ThreadFeederTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ########## (%s)' % os.path.basename(__file__))

    def tearDown(self):
        LOGGER.info('#############################')

    def make_feeder(self):

        # Mocked:
        self.thread = mock.MagicMock()
        self.feeder = mock.MagicMock()
        self.statemachine = esgfpid.rabbit.asynchronous.thread_statemachine.StateMachine()
        self.confirmer = esgfpid.rabbit.asynchronous.thread_confirmer.ConfirmReactor()
        feeder = esgfpid.rabbit.asynchronous.thread_feeder.RabbitFeeder(
            self.thread,
            self.statemachine,
            self.confirmer,
            'foo-exchange')
        self.statemachine._StateMachine__state = self.statemachine._StateMachine__IS_AVAILABLE
        return feeder

    def add_leftovers_to_confirmer(self):
        self.confirmer._ConfirmReactor__unconfirmed_delivery_tags = copy.copy(UNCONFIRMED_TAGS)
        self.confirmer._ConfirmReactor__unconfirmed_messages_dict = copy.copy(UNCONFIRMED_MESSAGES)

    # Tests

    #
    # Publish a message
    #

    def test_publish_message_ok(self):

        # Preparation:
        feeder = self.make_feeder()
        feeder._RabbitFeeder__unpublished_messages_queue.put('foo')

        # Run code to be tested:
        num1 = feeder.get_num_unpublished()
        feeder.publish_message()
        num2 = feeder.get_num_unpublished()

        # Check result
        self.assertEquals(num1, 1)
        self.assertEquals(num2, 0)
        self.assertEquals(feeder._RabbitFeeder__message_number, 1)


    def test_publish_message_queue_empty(self):

        # Preparation:
        feeder = self.make_feeder()

        # Run code to be tested:
        num1 = feeder.get_num_unpublished()
        feeder.publish_message()
        num2 = feeder.get_num_unpublished()

        # Check result
        self.assertEquals(num1, 0)
        self.assertEquals(num2, 0)
        self.assertEquals(feeder._RabbitFeeder__message_number, 0)


    def test_publish_while_wanting_to_stop_ok(self):

        # Preparation:
        feeder = self.make_feeder()
        feeder._RabbitFeeder__unpublished_messages_queue.put('foo')
        feeder.statemachine.set_to_wanting_to_stop()

        # Run code to be tested:
        num1 = feeder.get_num_unpublished()
        feeder.publish_message()
        num2 = feeder.get_num_unpublished()

        # Check result
        self.assertEquals(num1, 1)
        self.assertEquals(num2, 0)
        self.assertEquals(feeder._RabbitFeeder__message_number, 1)

    def test_publish_failed(self):

        # Preparation:
        feeder = self.make_feeder()
        feeder._RabbitFeeder__unpublished_messages_queue.put('foo')
        feeder.thread._channel.basic_publish.side_effect = AttributeError

        # Run code to be tested:
        num1 = feeder.get_num_unpublished()
        feeder.publish_message()
        num2 = feeder.get_num_unpublished()

        # Check result
        self.assertEquals(num1, 1)
        self.assertEquals(num2, 1) # None was published, so it stays 1
        self.assertEquals(feeder._RabbitFeeder__message_number, 0)


    def test_wrong_state_1a(self):

        # Preparation:
        feeder = self.make_feeder()
        feeder._RabbitFeeder__unpublished_messages_queue.put('foo')
        feeder.statemachine.set_to_permanently_unavailable()
        feeder.statemachine.could_not_connect = True

        # Run code to be tested:
        num1 = feeder.get_num_unpublished()
        feeder.publish_message()
        num2 = feeder.get_num_unpublished()

        # Check result
        self.assertEquals(num1, 1)
        self.assertEquals(num2, 1) # None was published, so it stays 1
        self.assertEquals(feeder._RabbitFeeder__message_number, 0)

    def test_wrong_state_1b(self):

        # Preparation:
        feeder = self.make_feeder()
        feeder._RabbitFeeder__unpublished_messages_queue.put('foo')
        feeder.statemachine.set_to_permanently_unavailable()
        feeder.statemachine.could_not_connect = False
        feeder.statemachine.closed_by_publisher = True

        # Run code to be tested:
        num1 = feeder.get_num_unpublished()
        feeder.publish_message()
        num2 = feeder.get_num_unpublished()

        # Check result
        self.assertEquals(num1, 1)
        self.assertEquals(num2, 1) # None was published, so it stays 1
        self.assertEquals(feeder._RabbitFeeder__message_number, 0)

    def test_wrong_state_2(self):

        # Preparation:
        feeder = self.make_feeder()
        feeder._RabbitFeeder__unpublished_messages_queue.put('foo')
        feeder.statemachine.set_to_waiting_to_be_available()

        # Run code to be tested:
        num1 = feeder.get_num_unpublished()
        feeder.publish_message()
        num2 = feeder.get_num_unpublished()

        # Check result
        self.assertEquals(num1, 1)
        self.assertEquals(num2, 1) # None was published, so it stays 1
        self.assertEquals(feeder._RabbitFeeder__message_number, 0)

    def test_wrong_state_3(self):

        # Preparation:
        feeder = self.make_feeder()
        feeder._RabbitFeeder__unpublished_messages_queue.put('foo')
        feeder.statemachine._StateMachine__state = self.statemachine._StateMachine__NOT_STARTED_YET

        # Run code to be tested:
        num1 = feeder.get_num_unpublished()
        feeder.publish_message()
        num2 = feeder.get_num_unpublished()

        # Check result
        self.assertEquals(num1, 1)
        self.assertEquals(num2, 1) # None was published, so it stays 1
        self.assertEquals(feeder._RabbitFeeder__message_number, 0)


    #
    # Putter
    #

    def test_put_message_into_queue_of_unsent_messages(self):

        # Preparation:
        feeder = self.make_feeder()

        # Run code to be tested:
        num1 = feeder.get_num_unpublished()
        feeder.put_message_into_queue_of_unsent_messages('foo')
        feeder.put_message_into_queue_of_unsent_messages('bar')
        feeder.put_message_into_queue_of_unsent_messages('baz')
        num2 = feeder.get_num_unpublished()

        # Check result
        self.assertEquals(num1, 0)
        self.assertEquals(num2, 3)
        received = feeder.get_unpublished_messages_as_list_copy()
        self.assertIn('foo', received)
        self.assertIn('bar', received)
        self.assertIn('baz', received)

    #
    # Getter
    #

    def test_get_unpublished_messages_as_list_copy(self):

        # Preparation:
        feeder = self.make_feeder()
        feeder._RabbitFeeder__unpublished_messages_queue.put('foo')
        feeder._RabbitFeeder__unpublished_messages_queue.put('bar')
        feeder._RabbitFeeder__unpublished_messages_queue.put('baz')

        # Run code to be tested:
        mylist = feeder.get_unpublished_messages_as_list_copy()

        # Check result
        self.assertIn('foo', mylist)
        self.assertIn('bar', mylist)
        self.assertIn('baz', mylist)


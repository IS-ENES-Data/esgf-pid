import unittest
import mock
import logging
import json
import pika
import esgfpid.rabbit.rabbit
import tests.mocks.rabbitmock
import tests.mocks.solrmock
import tests.json_utils

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from tests.resources.IGNORE.TESTVALUES_IGNORE import TESTVALUES_REAL

class RabbitIntegrationTestCase(unittest.TestCase):

    def make_rabbit_with_access(self):
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(
            exchange_name=TESTVALUES_REAL['exchange_name'],
            urls_fallback=TESTVALUES_REAL['url_messaging_service'],
            url_preferred=None,
            username=TESTVALUES_REAL['rabbit_username'],
            password=TESTVALUES_REAL['rabbit_password'],
        )
        return testrabbit

    # Tests

    def test_constructor_mandatory_args(self):
        testrabbit = self.make_rabbit_with_access()
        self.assertIsInstance(testrabbit, esgfpid.rabbit.rabbit.RabbitMessageSender)

    def test_constructor_wrong_credentials(self):
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(
            exchange_name=TESTVALUES_REAL['exchange_name'],
            urls_fallback=TESTVALUES_REAL['url_messaging_service'],
            url_preferred=None,
            username='wrong',
            password='wrong',
        )
        with self.assertRaises(pika.exceptions.ProbableAuthenticationError):
            testrabbit.open_rabbit_connection()


    def test_send_message_to_queue_ok(self):

        # Test variables:
        testrabbit = self.make_rabbit_with_access()
        routing_key = TESTVALUES_REAL['routing_key']
        message_body = '{"stuff":"foo"}'

        # Run code to be tested:
        delivered = testrabbit.send_message_to_queue(routing_key=routing_key, message=message_body)

        # Check result:
        self.assertTrue(delivered, 'Message not delivered!')

    def test_send_message_to_queue_wrong_routing_key(self):

        # Test variables:
        testrabbit = self.make_rabbit_with_access()
        routing_key = 'random'
        message_body = '{"stuff":"foo"}'

        # Run code to be tested:
        delivered = testrabbit.send_message_to_queue(routing_key=routing_key, message=message_body)

        # Check result:
        self.assertFalse(delivered, 'Message was delivered, but should not!')

    def test_send_message_to_queue_no_queue(self):

        # Test variables:
        routing_key = TESTVALUES_REAL['routing_key']
        message_body = '{"stuff":"foo"}'
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(
            exchange_name=TESTVALUES_REAL['exchange_no_queue'],
            urls_fallback=TESTVALUES_REAL['url_messaging_service'],
            url_preferred=None,
            username=TESTVALUES_REAL['rabbit_username'],
            password=TESTVALUES_REAL['rabbit_password'],
        )

        # Run code to be tested:
        delivered = testrabbit.send_message_to_queue(routing_key=routing_key, message=message_body)

        # Check result:
        self.assertFalse(delivered, 'Message was delivered, but should not!')

import unittest
import logging
import pika
import esgfpid.rabbit.rabbit
import time


# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from tests.resources.IGNORE.TESTVALUES_IGNORE import TESTVALUES_REAL

class RabbitIntegrationTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')

    def tearDown(self):
        LOGGER.info('#############################')

    def make_rabbit_with_access(self):
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(
            exchange_name=TESTVALUES_REAL['exchange_name'],
            urls_fallback=TESTVALUES_REAL['url_messaging_service'],
            url_preferred=None,
            username=TESTVALUES_REAL['rabbit_username'],
            password=TESTVALUES_REAL['rabbit_password'],
        )
        return testrabbit

    def make_rabbit_with_several_urls(self):
        testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(
            exchange_name=TESTVALUES_REAL['exchange_name'],
            #urls_fallback=['bar', TESTVALUES_REAL['url_messaging_service']],
            urls_fallback=['bar', 'baz'],
            url_preferred='foo',
            username=TESTVALUES_REAL['rabbit_username'],
            password=TESTVALUES_REAL['rabbit_password'],
        )
        return testrabbit

    # Tests


    if True: # WORKS
        def test_send_10_messages_to_queue_after_a_while_ok(self):
            '''
            This tests if messages are published correctly
            under normal conditions.

            The messages are sent after the thread had enough
            time to build the connection to the rabbit.
            '''

            # Test variables:
            routing_key = TESTVALUES_REAL['routing_key']
            num_messages = 10

            # Make connection:
            testrabbit = self.make_rabbit_with_access()
            testrabbit.start()

            # Wait for the connection to be established:
            time.sleep(5)

            # Run code to be tested:
            for i in xrange(num_messages):
                testrabbit.send_message_to_queue({"stuffy":"foo", "routing_key:": routing_key, "num":str(i+1)})

            # Close connection:
            LOGGER.debug('Sent. Closing now.')
            testrabbit.finish()

            # Check result:
            leftovers = testrabbit.get_leftovers()
            self.assertTrue(len(leftovers)==0, 'There is leftovers: '+str(len(leftovers)))
            self.assertTrue(testrabbit.is_finished(), 'Thread is still alive.')

    if True: # WORKS
        def test_several_urls_none_works(self):
            '''
            This tests if messages are published correctly
            under normal conditions.

            The messages are sent after the thread had enough
            time to build the connection to the rabbit.
            '''

            # Test variables:
            num_messages = 10
            routing_key = TESTVALUES_REAL['routing_key']
            message_body = '{"stuff":"foo"}'

            # Make connection:
            testrabbit = self.make_rabbit_with_several_urls()
            testrabbit.start()

            # Wait for the connection to be established:
            time.sleep(5)

            # Run code to be tested:
            for i in xrange(num_messages):
                testrabbit.send_message_to_queue({"stuffo":"foo","num":str(i+1)})

            # Close connection:
            testrabbit.finish()

            # Check result:
            leftovers = testrabbit.get_leftovers()
            self.assertTrue(len(leftovers)==num_messages, 'No message should have been sent, but there is '+str(len(leftovers))+' instead of '+str(num_messages))
            self.assertTrue(testrabbit.is_finished(), 'Thread is still alive.')

    if True:

        def test_send_10_messages_to_queue_quickly_ok(self):
            '''
            This tests if messages which cannot be sent immediately
            (because the connection is not ready) are sent later.

            The messages are published immediately after init of
            the sender, when the chain to build the connection is
            not ready yet.
            The method __check_for_already_arrived_messages() should
            find and publish them once the connection is built.
            '''

            # Test variables:
            routing_key = TESTVALUES_REAL['routing_key']
            num_messages = 10

            # Make connection:
            testrabbit = self.make_rabbit_with_access()
            testrabbit.start()

            # Run code to be tested:
            for i in xrange(num_messages):
                testrabbit.send_message_to_queue({"stuffee":"foo","num":str(i+1)})

            # Close connection after some time:
            time.sleep(5)
            testrabbit.finish()

            # Check result:
            leftovers = testrabbit.get_leftovers()
            self.assertTrue(len(leftovers)==0, 'There is leftovers: '+str(len(leftovers)))
            self.assertTrue(testrabbit.is_finished(), 'Thread is still alive.')

    if True:

        def test_send_100_messages_to_queue_and_force_finish(self):

            # Test variables:
            routing_key = TESTVALUES_REAL['routing_key']
            num_messages = 100

            # Make connection:
            testrabbit = self.make_rabbit_with_access()
            testrabbit.start()
            time.sleep(0.1)

            # Run code to be tested:
            for i in xrange(num_messages):
                testrabbit.send_message_to_queue({"stuff":"foo","num":str(i+1)})

            # Close connection after some time:
            testrabbit.force_finish()

            # Check result:
            leftovers = testrabbit.get_leftovers()
            self.assertTrue(len(leftovers)>0, 'There is no leftovers! There should be!')
            self.assertTrue(testrabbit.is_finished(), 'Thread is still alive.')

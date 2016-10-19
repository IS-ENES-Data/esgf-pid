import unittest
import mock
import logging
import json
import esgfpid.assistant.shoppingcart
import tests.mocks.rabbitmock
import tests.mocks.solrmock
import tests.utils as utils
from tests.utils import compare_json_return_errormessage as error_message
from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS


# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import TESTVALUES as TESTVALUES

class ShoppingCartTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')

    def tearDown(self):
        LOGGER.info('#############################')

    def make_test_coupler_for_sending_messages(self):
        testcoupler = esgfpid.coupling.Coupler(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_urls = TESTVALUES['url_messaging_service'],
            messaging_service_url_preferred = TESTVALUES['url_messaging_service'],
            messaging_service_exchange_name = TESTVALUES['messaging_exchange'],
            solr_url = TESTVALUES['solr_url'],
            messaging_service_username = TESTVALUES['rabbit_username'],
            messaging_service_password = TESTVALUES['rabbit_password'],
            solr_switched_off = True,
            solr_https_verify=False
        )
        # Replace objects that interact with servers with mocks
        testcoupler._Coupler__rabbit_message_sender = tests.mocks.rabbitmock.SimpleMockRabbitSender()
        return testcoupler

    def __get_received_message_from_rabbit_mock(self, coupler, index):
        return coupler._Coupler__rabbit_message_sender.received_messages[index]

    ### Actual test cases: ###

    def test_init_ok(self):

        # Test variables
        testcoupler = self.make_test_coupler_for_sending_messages()
        prefix = TESTVALUES['prefix']
  
        # Run code to be tested:
        assistant = esgfpid.assistant.shoppingcart.ShoppingCartAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Check result:
        self.assertIsInstance(assistant, esgfpid.assistant.shoppingcart.ShoppingCartAssistant, 'Constructor fail.')

    def test_add_content_one_as_string(self):

        # Test variables
        testcoupler = self.make_test_coupler_for_sending_messages()
        prefix = TESTVALUES['prefix']
  
        # Preparations
        assistant = esgfpid.assistant.shoppingcart.ShoppingCartAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.make_shopping_cart_pid('foo')

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+prefix+'/a5bf60bd-fe2d-3fac-bbd7-404751e6ca66',
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":['foo'],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_content_several(self):

        # Test variables
        testcoupler = self.make_test_coupler_for_sending_messages()
        prefix = TESTVALUES['prefix']
        content1 = ['foo', 'hdl:bar', 'hdl:BAZ']
        content2 = ['baz', 'bar', 'foo']

        # Preparations
        assistant = esgfpid.assistant.shoppingcart.ShoppingCartAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        pid1 = assistant.make_shopping_cart_pid(content1)
        received_rabbit_task1 = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        pid2 = assistant.make_shopping_cart_pid(content2)
        received_rabbit_task2 = self.__get_received_message_from_rabbit_mock(testcoupler, 1)

        # Check result:
        expected_handle_both_cases = "hdl:"+prefix+"/27785cdf-bae8-3fd1-857a-58399ab16385"
        expected_rabbit_task1 = {
            "handle": expected_handle_both_cases,
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":['foo', 'hdl:bar', 'hdl:BAZ'],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        expected_rabbit_task2 = {
            "handle": expected_handle_both_cases,
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":['baz', 'bar', 'foo'],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        same1 = utils.is_json_same(expected_rabbit_task1, received_rabbit_task1)
        same2 = utils.is_json_same(expected_rabbit_task2, received_rabbit_task2)
        self.assertTrue(same1, error_message(expected_rabbit_task1, received_rabbit_task1))
        self.assertTrue(same2, error_message(expected_rabbit_task2, received_rabbit_task2))
        self.assertTrue(pid1==pid2, 'Both pids are not the same.')

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
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def make_test_coupler_for_sending_messages(self):
        testcoupler = esgfpid.coupling.Coupler(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_urls_open = TESTVALUES['url_rabbit_open'],
            messaging_service_url_trusted = TESTVALUES['url_rabbit_trusted'],
            messaging_service_exchange_name = TESTVALUES['rabbit_exchange_name'],
            solr_url = TESTVALUES['solr_url'],
            messaging_service_username_open = TESTVALUES['rabbit_username_open'],
            messaging_service_username_trusted = TESTVALUES['rabbit_username_trusted'],
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

    def test_add_content_several(self):

        # Test variables
        testcoupler = self.make_test_coupler_for_sending_messages()
        prefix = TESTVALUES['prefix']
  
        # Preparations
        assistant = esgfpid.assistant.shoppingcart.ShoppingCartAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.make_shopping_cart_pid({'foo':'foo', 'bar':'bar'})

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+prefix+'/339427df-edbd-3f43-acf2-80ddc7729f27',
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":{'foo':'foo', 'bar':'bar'},
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_content_some_none(self):

        # Test variablesl
        testcoupler = self.make_test_coupler_for_sending_messages()
        prefix = TESTVALUES['prefix']
  
        # Preparations
        assistant = esgfpid.assistant.shoppingcart.ShoppingCartAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.make_shopping_cart_pid({'foo':'foo', 'bar':None})

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+prefix+'/339427df-edbd-3f43-acf2-80ddc7729f27',
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":{'foo':'foo', 'bar':None},
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_content_several_old(self):

        # Test variables
        testcoupler = self.make_test_coupler_for_sending_messages()
        prefix = TESTVALUES['prefix']
        content1 = {'foo':'foo', 'bar':'bar'}
        content2 = {'foo':'foo', 'bar': None}
        content3 = {'foo':'foo', 'bar':'hdl:bar'}


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
        pid3 = assistant.make_shopping_cart_pid(content3)
        received_rabbit_task3 = self.__get_received_message_from_rabbit_mock(testcoupler, 2)

        # Check result:
        expected_handle_both_cases = "hdl:"+prefix+"/339427df-edbd-3f43-acf2-80ddc7729f27"
        expected_rabbit_task1 = {
            "handle": expected_handle_both_cases,
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":content1,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        expected_rabbit_task2 = {
            "handle": expected_handle_both_cases,
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":content2,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        expected_rabbit_task3 = {
            "handle": expected_handle_both_cases,
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":content3,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        same1 = utils.is_json_same(expected_rabbit_task1, received_rabbit_task1)
        same2 = utils.is_json_same(expected_rabbit_task2, received_rabbit_task2)
        same3 = utils.is_json_same(expected_rabbit_task3, received_rabbit_task3)
        self.assertTrue(same1, error_message(expected_rabbit_task1, received_rabbit_task1))
        self.assertTrue(same2, error_message(expected_rabbit_task2, received_rabbit_task2))
        self.assertTrue(same3, error_message(expected_rabbit_task3, received_rabbit_task3))
        self.assertTrue(pid1==pid2, 'Pids 1&2 are not the same.')
        self.assertTrue(pid1==pid3, 'Pids 1&3 are not the same.')

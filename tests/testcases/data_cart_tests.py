import unittest
import mock
import logging
import json
import esgfpid.assistant.datacart
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

class DataCartTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def make_test_coupler_for_sending_messages(self):
        rabbit_creds_open = {
            "url":TESTVALUES['url_rabbit_open'],
            "user":TESTVALUES['rabbit_username_open']
        }
        rabbit_creds_trusted = {
            "url":TESTVALUES['url_rabbit_trusted'],
            "user":TESTVALUES['rabbit_username_trusted'],
            "password":TESTVALUES['rabbit_password']
        }
        testcoupler = esgfpid.coupling.Coupler(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_credentials = [rabbit_creds_trusted, rabbit_creds_open],
            messaging_service_exchange_name = TESTVALUES['rabbit_exchange_name'],
            solr_url = TESTVALUES['solr_url'],
            solr_switched_off = True,
            solr_https_verify=False
        )
        # Replace objects that interact with servers with mocks
        testcoupler._Coupler__rabbit_message_sender = tests.mocks.rabbitmock.SimpleMockRabbitSender()
        return testcoupler

    def __get_received_message_from_rabbit_mock(self, coupler, index):
        return coupler._Coupler__rabbit_message_sender.received_messages[index]

    ### Actual test cases: ###

    '''
    Trivial test of the constructor.
    Only check if instantiation works without errors.
    '''
    def test_init_ok(self):

        # Test variables
        prefix = 'myprefix'

        # Preparation: Make patched test coupler
        testcoupler = self.make_test_coupler_for_sending_messages()

        # Run code to be tested:
        assistant = esgfpid.assistant.datacart.DataCartAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Check result:
        self.assertIsInstance(assistant, esgfpid.assistant.datacart.DataCartAssistant, 'Constructor fail.')

    '''
    Make sure that the PID of a datacart with just one dataset
    is not the same as the dataset PID itself!
    '''
    def test_if_datacart_pid_same_as_dataset_pid(self):

        # Test variables:
        prefix = 'myprefix'
        drs_id = 'mydrsfoo'
        vers_num = 20160101
        # Dataset id and PID
        dataset_id = esgfpid.utils.concatenate_drs_and_versionnumber(
            drs_id,
            vers_num
        )
        dataset_pid = esgfpid.assistant.publish.create_dataset_handle(
            drs_id = drs_id,
            version_number = vers_num,
            prefix = prefix
        )
        # Data cart content:
        content = {dataset_id : dataset_pid}

        # Preparations: Make an assistant with a patched coupler.
        testcoupler = self.make_test_coupler_for_sending_messages()
        assistant = esgfpid.assistant.datacart.DataCartAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        datacart_pid = assistant.make_data_cart_pid(content)

        # Check result
        self.assertNotEquals(dataset_pid, datacart_pid)

    '''
    Test if we can create a data cart PID for several
    datasets that all have PIDs.
    '''
    def test_datacart_for_several_datasets_with_pids(self):

        # Test variables
        prefix = 'myprefix'
        content = {'foo':'foo', 'bar':'bar'}
  
        # Preparations: Make an assistant with a patched coupler.
        testcoupler = self.make_test_coupler_for_sending_messages()
        assistant = esgfpid.assistant.datacart.DataCartAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.make_data_cart_pid(content)

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+prefix+'/b597a79e-1dc7-3d3f-b689-75ac5a78167f',
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":{'foo':'foo', 'bar':'bar'},
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    '''
    Test whether we can create a data cart PID even
    if some datasets have no PID.
    '''
    def test_datacart_some_datasets_have_no_handles(self):

        # Test variablesl
        prefix = 'myprefix'
        content = {'foo':'foo', 'bar':None}
  
        # Preparations: Make an assistant with a patched coupler.
        testcoupler = self.make_test_coupler_for_sending_messages()
        assistant = esgfpid.assistant.datacart.DataCartAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.make_data_cart_pid(content)

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+prefix+'/b597a79e-1dc7-3d3f-b689-75ac5a78167f',
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content": content,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    '''
    Test whether the same data cart PID is created/updated, if
    the same datasets are given, even if dataset PIDs may vary
    (with/without "hdl:", or nonexistent handle strings).
    '''
    def test_datacart_several_times_same_datasets(self):

        # Test variables
        prefix = 'myprefix'
        content1 = {'foo':'foo', 'bar':'bar'}
        content2 = {'foo':'foo', 'bar': None}
        content3 = {'foo':'foo', 'bar':'hdl:bar'}
        # Note: In all three cases, the same datasets are passed, so
        # the same data cart PID has to be created/updated.

        # Preparations: Make an assistant with a patched coupler.
        testcoupler = self.make_test_coupler_for_sending_messages()
        assistant = esgfpid.assistant.datacart.DataCartAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        pid1 = assistant.make_data_cart_pid(content1)
        received_rabbit_task1 = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        pid2 = assistant.make_data_cart_pid(content2)
        received_rabbit_task2 = self.__get_received_message_from_rabbit_mock(testcoupler, 1)
        pid3 = assistant.make_data_cart_pid(content3)
        received_rabbit_task3 = self.__get_received_message_from_rabbit_mock(testcoupler, 2)

        # Check result:
        # These are the expected messages:
        expected_handle_all_cases = "hdl:"+prefix+"/b597a79e-1dc7-3d3f-b689-75ac5a78167f"
        expected_rabbit_task1 = {
            "handle": expected_handle_all_cases,
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":content1,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        expected_rabbit_task2 = {
            "handle": expected_handle_all_cases,
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":content2,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        expected_rabbit_task3 = {
            "handle": expected_handle_all_cases,
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":content3,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        # Check if all the messages are correct:
        same1 = utils.is_json_same(expected_rabbit_task1, received_rabbit_task1)
        same2 = utils.is_json_same(expected_rabbit_task2, received_rabbit_task2)
        same3 = utils.is_json_same(expected_rabbit_task3, received_rabbit_task3)
        self.assertTrue(same1, error_message(expected_rabbit_task1, received_rabbit_task1))
        self.assertTrue(same2, error_message(expected_rabbit_task2, received_rabbit_task2))
        self.assertTrue(same3, error_message(expected_rabbit_task3, received_rabbit_task3))
        # Check if all the handles are the same:
        self.assertTrue(pid1==pid2, 'Pids 1&2 are not the same.')
        self.assertTrue(pid1==pid3, 'Pids 1&3 are not the same.')

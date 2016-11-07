import unittest
import mock
import logging
import json
import esgfpid.assistant.errata
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

class ErrataTestCase(unittest.TestCase):

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
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        prefix = TESTVALUES['prefix']
        errata_id = TESTVALUES['errata_id']
  
        # Run code to be tested:
        assistant = esgfpid.assistant.errata.ErrataAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Check result:
        self.assertIsInstance(assistant, esgfpid.assistant.errata.ErrataAssistant, 'Constructor fail.')

    def test_add_errata_id_one_as_string(self):

        # Test variables
        testcoupler = self.make_test_coupler_for_sending_messages()
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        prefix = TESTVALUES['prefix']
        errata_id = TESTVALUES['errata_id']
  
        # Preparations
        assistant = esgfpid.assistant.errata.ErrataAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.add_errata_ids(
            drs_id=drs_id,
            version_number=version_number,
            errata_ids=errata_id
        )

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+prefix+'/'+TESTVALUES['suffix1'],
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":[errata_id],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":drs_id,
            "version_number":version_number
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_errata_id_one_as_list(self):

        # Test variables
        testcoupler = self.make_test_coupler_for_sending_messages()
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        prefix = TESTVALUES['prefix']
        errata_id = [TESTVALUES['errata_id']]
  
        # Preparations
        assistant = esgfpid.assistant.errata.ErrataAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.add_errata_ids(
            drs_id=drs_id,
            version_number=version_number,
            errata_ids=errata_id
        )

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+prefix+'/'+TESTVALUES['suffix1'],
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":errata_id,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":drs_id,
            "version_number":version_number
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_errata_id_several(self):

        # Test variables
        testcoupler = self.make_test_coupler_for_sending_messages()
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        prefix = TESTVALUES['prefix']
        errata_ids = TESTVALUES['errata_ids']
  
        # Preparations
        assistant = esgfpid.assistant.errata.ErrataAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.add_errata_ids(
            drs_id=drs_id,
            version_number=version_number,
            errata_ids=errata_ids
        )

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+prefix+'/'+TESTVALUES['suffix1'],
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":errata_ids,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":drs_id,
            "version_number":version_number
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_errata_id_missing_args(self):

        # Test variables
        testcoupler = self.make_test_coupler_for_sending_messages()
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        prefix = TESTVALUES['prefix']
        errata_ids = TESTVALUES['errata_ids']
  
        # Preparations
        assistant = esgfpid.assistant.errata.ErrataAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant.add_errata_ids(
                drs_id=drs_id,
                version_number=version_number
            )

    def test_remove_errata_id_several(self):

        # Test variables
        testcoupler = self.make_test_coupler_for_sending_messages()
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        prefix = TESTVALUES['prefix']
        errata_ids = TESTVALUES['errata_ids']
  
        # Preparations
        assistant = esgfpid.assistant.errata.ErrataAssistant(
            prefix=prefix,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.remove_errata_ids(
            drs_id=drs_id,
            version_number=version_number,
            errata_ids=errata_ids
        )

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+prefix+'/'+TESTVALUES['suffix1'],
            "operation": "remove_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":errata_ids,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.remove',
            "drs_id":drs_id,
            "version_number":version_number
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))
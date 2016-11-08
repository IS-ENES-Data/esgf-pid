import unittest
import mock
import logging
import json
import sys
import esgfpid.assistant.unpublish
import tests.mocks.rabbitmock
import tests.mocks.solrmock
import tests.utils
from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class UnpublicationTestCase(unittest.TestCase):

    def tearDown(self):
        LOGGER.info('#############################')

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)
        self.prefix = '123456'
        self.data_node = 'my.data.node'
        self.drs_id = 'mytest'
        self.coupler = self.make_coupler_with_defaults_and_mocks()

    @mock.patch('esgfpid.rabbit.rabbit.RabbitMessageSender.__init__', mock.Mock(return_value=None))
    @mock.patch('esgfpid.solr.solr.SolrInteractor.__init__', mock.Mock(return_value=None))
    def make_coupler_with_defaults_and_mocks(self):

        # Make coupler
        args = self.get_args_for_coupler()
        testcoupler = esgfpid.coupling.Coupler(**args)

        # Replace objects that interact with servers with mocks
        self.__patch_coupler_with_solr_mock(testcoupler)
        self.__patch_coupler_with_rabbit_mock(testcoupler)
        return testcoupler

    def __patch_coupler_with_solr_mock(self, coupler, solrmock=None):
        if solrmock is None:
            solrmock = tests.mocks.solrmock.MockSolrInteractor()
        coupler._Coupler__solr_sender = solrmock

    def __patch_coupler_with_rabbit_mock(self, coupler, rabbitmock=None):
        if rabbitmock is None:
            rabbitmock = tests.mocks.rabbitmock.SimpleMockRabbitSender()
        coupler._Coupler__rabbit_message_sender = rabbitmock

    def __get_received_message_from_rabbit_mock(self, coupler, index):
        return coupler._Coupler__rabbit_message_sender.received_messages[index]

    def get_args_for_coupler(self):
        args = dict(
            handle_prefix=self.prefix,
            messaging_service_urls_open='www.rabbit.foo',
            messaging_service_url_trusted='www.trusted-rabbit.foo',
            messaging_service_exchange_name='exch',
            messaging_service_username_open='rogerRabbit',
            messaging_service_username_trusted='rogerRabbit',
            messaging_service_password='mySecretCarrotDream',
            solr_url='foo.solr.bar',
            solr_https_verify=False,
            solr_switched_off=True
        )
        return args

    def make_assistant_one_version(self):
        assistant = esgfpid.assistant.unpublish.AssistantOneVersion(
            drs_id=self.drs_id,
            prefix=self.prefix,
            coupler=self.coupler,
            data_node=self.data_node,
            message_timestamp='some_moment'
        )
        return assistant

    def make_assistant_all_versions(self, coupler=None):
        assistant = esgfpid.assistant.unpublish.AssistantAllVersions(
            drs_id=self.drs_id,
            prefix=self.prefix,
            coupler=coupler or self.coupler,
            data_node=self.data_node,
            message_timestamp='some_moment',
            consumer_solr_url='fake_solr_foo'
        )
        return assistant

    # Tests

    def test_unpublish_one_version_by_version_number(self):

        # Test variables
        version_number='999888'

        # Preparations
        assistant = self.make_assistant_one_version()
  
        # Run code to be tested:
        assistant.unpublish_one_dataset_version(
            version_number=version_number
        )

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+self.prefix+"/8bce9b3c-88eb-3f6d-a409-4f69ef7d043f",
            "aggregation_level": "dataset",
            "operation": "unpublish_one_version",
            "message_timestamp":"anydate",
            "data_node": self.data_node,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id":self.drs_id,
            "version_number":int(version_number)
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(self.coupler, 0)

        messagesOk = tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task)
        self.assertIsNone(messagesOk, messagesOk)

    def test_unpublish_one_version_by_version_number_and_handle(self):

        # Test variables
        version_number='999888'
        handle = 'hdl:'+self.prefix + '/8bce9b3c-88eb-3f6d-a409-4f69ef7d043f' # is redundant, but will be checked.
  
        # Preparations
        assistant = self.make_assistant_one_version()

        # Run code to be tested:
        assistant.unpublish_one_dataset_version(
            version_number=version_number,
            dataset_handle=handle
        )

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+self.prefix+"/8bce9b3c-88eb-3f6d-a409-4f69ef7d043f",
            "aggregation_level": "dataset",
            "operation": "unpublish_one_version",
            "message_timestamp":"anydate",
            "data_node": self.data_node,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id":self.drs_id,
            "version_number":int(version_number)
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(self.coupler, 0)
        messagesOk = tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task)
        self.assertIsNone(messagesOk, messagesOk)

    def test_unpublish_one_version_wrong_handle(self):

        # Test variables
        version_number = '999888'
        wrong_handle = self.prefix+'/miauz'
  
        # Preparations
        assistant = self.make_assistant_one_version()

        # Run code to be tested and check exception:
        with self.assertRaises(ValueError):
            assistant.unpublish_one_dataset_version(version_number=version_number, dataset_handle=wrong_handle)

    def test_unpublish_one_version_version_is_none(self):
        
        # Preparations
        assistant = self.make_assistant_one_version()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant.unpublish_one_dataset_version(version_number=None)

    def test_unpublish_one_version_no_version_given(self):
        
        # Preparations
        assistant = self.make_assistant_one_version()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant.unpublish_one_dataset_version()

    def test_unpublish_all_versions_nosolr_consumer_must_find_versions_ok(self):        

        # Preparations:
        assistant = self.make_assistant_all_versions()

        # Run code to be tested:
        assistant.unpublish_all_dataset_versions()

        # Check result:
        expected_rabbit_task = {
            "operation": "unpublish_all_versions",
            "aggregation_level": "dataset",
            "message_timestamp": "anydate",
            "drs_id": self.drs_id,
            "data_node": self.data_node,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all',
            "consumer_solr_url":"fake_solr_foo"
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(self.coupler, 0)
        messagesOk = tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task)
        self.assertIsNone(messagesOk, messagesOk)

    def test_unpublish_all_versions_solr_off_consumer_must_find_versions_ok(self):        

        # Preparations:
        # Make coupler
        args = self.get_args_for_coupler()
        args['solr_url']=None
        args['solr_switched_off']=True
        testcoupler = esgfpid.coupling.Coupler(**args)
        # Replace objects that interact with servers with mocks
        self.__patch_coupler_with_rabbit_mock(testcoupler)
        # Make assistant
        assistant = self.make_assistant_all_versions(testcoupler)

        # Run code to be tested:
        assistant.unpublish_all_dataset_versions()

        # Check result:
        expected_rabbit_task = {
            "operation": "unpublish_all_versions",
            "aggregation_level": "dataset",
            "message_timestamp": "anydate",
            "drs_id": self.drs_id,
            "data_node": self.data_node,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all',
            "consumer_solr_url":"fake_solr_foo"
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        messagesOk = tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task)
        self.assertIsNone(messagesOk, messagesOk)

    def test_unpublish_all_versions_nosolr_consumer_must_find_versions_solr_none(self):        

        # Preparations:
        assistant = esgfpid.assistant.unpublish.AssistantAllVersions(
            drs_id=self.drs_id,
            prefix=self.prefix,
            coupler=self.coupler,
            data_node=self.data_node,
            message_timestamp='some_moment',
            consumer_solr_url=None
        )

        # Run code to be tested:
        assistant.unpublish_all_dataset_versions()

        # Check result:
        expected_rabbit_task = {
            "operation": "unpublish_all_versions",
            "aggregation_level": "dataset",
            "message_timestamp": "anydate",
            "drs_id": self.drs_id,
            "data_node": self.data_node,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all'
        }
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(self.coupler, 0)
        messagesOk = tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task)
        self.assertIsNone(messagesOk, messagesOk)

    def test_unpublish_all_versions_nosolr_consumer_must_find_versions_no_solr(self):        

        # Check exception:

        assistant = esgfpid.assistant.unpublish.AssistantAllVersions(
            drs_id=self.drs_id,
            prefix=self.prefix,
            coupler=self.coupler,
            data_node=self.data_node,
            message_timestamp='some_moment',
        )

    def test_unpublish_all_versions_by_handles_ok(self):

        # Test variables
        list_of_handles = [self.prefix+'/bla', self.prefix+'/blub']

        # Set solr mock to return handles:
        self.coupler._Coupler__solr_sender.datasethandles_or_versionnumbers_of_allversions['dataset_handles'] = list_of_handles
  
        # Preparations:
        assistant = self.make_assistant_all_versions()

        # Run code to be tested:
        assistant.unpublish_all_dataset_versions()

        # Check result:
        expected_rabbit_task_0 = {
            "operation": "unpublish_one_version",
            "aggregation_level": "dataset",
            "message_timestamp": "anydate",
            "data_node": self.data_node,
            "handle":list_of_handles[0],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id": self.drs_id
        }
        expected_rabbit_task_1 = {
            "operation": "unpublish_one_version",
            "aggregation_level": "dataset",
            "message_timestamp": "anydate",
            "data_node": self.data_node,
            "handle":list_of_handles[1],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id": self.drs_id
        }
        received_rabbit_task_0 = self.__get_received_message_from_rabbit_mock(self.coupler, 0)
        received_rabbit_task_1 = self.__get_received_message_from_rabbit_mock(self.coupler, 1)

        messagesOk = tests.utils.compare_json_return_errormessage(expected_rabbit_task_0, received_rabbit_task_0)
        self.assertIsNone(messagesOk, messagesOk)

        messagesOk = tests.utils.compare_json_return_errormessage(expected_rabbit_task_1, received_rabbit_task_1)
        self.assertIsNone(messagesOk, messagesOk)

    def test_unpublish_all_versions_by_version_numbers_ok(self):
        
        # Test variables
        list_of_version_numbers = [12345, 54321]

        # Set solr mock to return handles:
        self.coupler._Coupler__solr_sender.datasethandles_or_versionnumbers_of_allversions['version_numbers'] = list_of_version_numbers
  
        # Preparations:
        assistant = self.make_assistant_all_versions()

        # Run code to be tested:
        assistant.unpublish_all_dataset_versions()

        # Check result:
        expected_rabbit_task_0 = {
            "operation": "unpublish_one_version",
            "aggregation_level": "dataset",
            "message_timestamp": "anydate",
            "data_node": self.data_node,
            "handle": "hdl:"+self.prefix+"/68f3d5ab-bddd-3701-abf4-4b24ccd54525",
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id": self.drs_id,
            "version_number": 12345
        }
        expected_rabbit_task_1 = {
            "operation": "unpublish_one_version",
            "aggregation_level": "dataset",
            "message_timestamp": "anydate",
            "data_node": self.data_node,
            "handle": "hdl:"+self.prefix+"/fca5741d-89c1-3149-a34d-c7fb37be75a8",
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id": self.drs_id,
            "version_number": 54321
        }
        received_rabbit_task_0 = self.__get_received_message_from_rabbit_mock(self.coupler, 0)
        received_rabbit_task_1 = self.__get_received_message_from_rabbit_mock(self.coupler, 1)

        messagesOk = tests.utils.compare_json_return_errormessage(expected_rabbit_task_0, received_rabbit_task_0)
        self.assertIsNone(messagesOk, messagesOk)

        messagesOk = tests.utils.compare_json_return_errormessage(expected_rabbit_task_1, received_rabbit_task_1)
        self.assertIsNone(messagesOk, messagesOk)

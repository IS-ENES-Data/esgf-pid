import unittest
import mock
import logging
import json
import sys
import esgfpid.assistant.publish
import tests.utils
from tests.utils import compare_json_return_errormessage as error_message
import tests.mocks.rabbitmock
import tests.mocks.solrmock
from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS
import esgfpid.utils

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import TESTVALUES


class ApiTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')
        self.make_mocks()

    def tearDown(self):
        LOGGER.info('#############################')

    def make_mocks(self):
        self.default_solrmock = mock.MagicMock()
        self.default_solrmock.is_switched_off.return_value = False
        self.default_rabbitmock = mock.MagicMock()
        self.default_testconnector = self.make_patched_connector()

    def make_patched_connector(self):
        testconnector = esgfpid.Connector(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_urls = TESTVALUES['url_messaging_service'],
            messaging_service_exchange_name = TESTVALUES['messaging_exchange'],
            data_node = TESTVALUES['data_node'],
            thredds_service_path = TESTVALUES['thredds_service_path'], # opt
            solr_url = TESTVALUES['solr_url'],
            messaging_service_username = TESTVALUES['rabbit_username'],
            messaging_service_password = TESTVALUES['rabbit_password']
        )
        # Replace objects that interact with servers with mocks
        self.__patch_connector_with_solr_mock(testconnector)
        self.__patch_connector_with_rabbit_mock(testconnector)
        return testconnector

    def __patch_connector_with_solr_mock(self, connector, solrmock=None):
        if solrmock is None:
            solrmock = self.default_solrmock
        connector._Connector__coupler._Coupler__solr_sender = solrmock

    def __patch_connector_with_rabbit_mock(self, connector, rabbitmock=None):
        if rabbitmock is None:
            rabbitmock = self.default_rabbitmock
        connector._Connector__coupler._Coupler__rabbit_message_sender = rabbitmock

    # Actual tests:

    #
    # Init
    #

    def test_init_ok(self):

        testconnector = esgfpid.Connector(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_urls = TESTVALUES['url_messaging_service'],
            messaging_service_exchange_name = TESTVALUES['messaging_exchange'],
            data_node = TESTVALUES['data_node'],
            thredds_service_path = TESTVALUES['thredds_service_path'], # opt
            solr_url = TESTVALUES['solr_url'],
            messaging_service_username = TESTVALUES['rabbit_username'],
            messaging_service_password = TESTVALUES['rabbit_password']
        )

        self.assertIsInstance(testconnector, esgfpid.Connector)

    def test_init_ok_no_rabbit_pw(self):

        testconnector = esgfpid.Connector(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_urls = TESTVALUES['url_messaging_service'],
            messaging_service_exchange_name = TESTVALUES['messaging_exchange'],
            data_node = TESTVALUES['data_node'],
            thredds_service_path = TESTVALUES['thredds_service_path'], # opt
            solr_url = TESTVALUES['solr_url'],
            messaging_service_username = TESTVALUES['rabbit_username']
        )

        self.assertIsInstance(testconnector, esgfpid.Connector)


    def test_init_with_consumer_solr_url_ok(self):

        testconnector = esgfpid.Connector(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_urls = TESTVALUES['url_messaging_service'],
            messaging_service_exchange_name = TESTVALUES['messaging_exchange'],
            data_node = TESTVALUES['data_node'],
            thredds_service_path = TESTVALUES['thredds_service_path'], # opt
            solr_url = TESTVALUES['solr_url'],
            messaging_service_username = TESTVALUES['rabbit_username'],
            messaging_service_password = TESTVALUES['rabbit_password'],
            consumer_solr_url='fake_solr_whatever'
        )

        self.assertIsInstance(testconnector, esgfpid.Connector)

    #
    # Publication
    #

    def test_create_publication_assistant_ok(self):

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        data_node = TESTVALUES['data_node']
        is_replica = False
        thredds_service_path = TESTVALUES['thredds_service_path']

        # Run code to be tested:
        wizard = self.default_testconnector.create_publication_assistant(
            drs_id=drs_id,
            version_number=version_number,
            data_node=data_node,
            thredds_service_path=thredds_service_path,
            is_replica=is_replica
        )

        # Check result:
        self.assertIsInstance(wizard, esgfpid.assistant.publish.DatasetPublicationAssistant,
            'Is no instance!')

    def test_create_publication_assistant_missing_thredds(self):

        # Preparations: Make connector without thredds
        testconnector = esgfpid.Connector(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_urls = TESTVALUES['url_messaging_service'],
            messaging_service_exchange_name = TESTVALUES['messaging_exchange'],
            data_node = TESTVALUES['data_node'],
            solr_url = TESTVALUES['solr_url'],
            messaging_service_username = TESTVALUES['rabbit_username'],
            messaging_service_password = TESTVALUES['rabbit_password']
        )

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        data_node = TESTVALUES['data_node']
        is_replica = False

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            wizard = testconnector.create_publication_assistant(
                drs_id=drs_id,
                version_number=version_number,
                data_node=data_node,
                is_replica=is_replica
            )

    #
    # Unpublication
    #

    def test_unpublish_one_version_ok(self):

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        data_node = TESTVALUES['data_node']
  
        # Run code to be tested:
        self.default_testconnector.unpublish_one_version(
            drs_id=drs_id,
            version_number=version_number,
            data_node=data_node)

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+TESTVALUES['prefix']+'/afd65cd0-9296-35bc-a706-be98665c9c36',
            "operation": "unpublish_one_version",
            "message_timestamp":"anydate",
            "aggregation_level":"dataset",
            "data_node": TESTVALUES['data_node'],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one'
        }
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    def test_unpublish_one_version_with_consumer_url_ok(self):

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        data_node = TESTVALUES['data_node']

        # Make test connector
        testconnector = esgfpid.Connector(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_urls = TESTVALUES['url_messaging_service'],
            messaging_service_exchange_name = TESTVALUES['messaging_exchange'],
            data_node = TESTVALUES['data_node'],
            solr_url = TESTVALUES['solr_url'],
            messaging_service_username = TESTVALUES['rabbit_username'],
            messaging_service_password = TESTVALUES['rabbit_password'],
            consumer_solr_url="fake_solr_whatever"
        )
        self.__patch_connector_with_rabbit_mock(testconnector)
  
        # Run code to be tested:
        testconnector.unpublish_one_version(
            drs_id=drs_id,
            version_number=version_number,
            data_node=data_node)

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+TESTVALUES['prefix']+'/afd65cd0-9296-35bc-a706-be98665c9c36',
            "operation": "unpublish_one_version",
            "message_timestamp":"anydate",
            "aggregation_level":"dataset",
            "data_node": TESTVALUES['data_node'],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one'
        } # We don't get the consumer_solr_url, because it is only needed for all versions.
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    @mock.patch('esgfpid.coupling.Coupler.retrieve_datasethandles_or_versionnumbers_of_allversions')
    def test_unpublish_all_versions_nosolr_ok(self, solr_asker_patch):

        # Patch coupler
        mydict = dict(dataset_handles=None, version_numbers=None)
        solr_asker_patch.return_value = mydict

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        data_node = TESTVALUES['data_node']

        # Run code to be tested:
        self.default_testconnector.unpublish_all_versions(
            drs_id=drs_id,
            data_node=data_node)

        # Check result:
        expected_rabbit_task = {
            "operation": "unpublish_all_versions",
            "message_timestamp": "anydate",
            "data_node": TESTVALUES['data_node'],
            "aggregation_level":"dataset",
            "drs_id":drs_id,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all'
        }
        
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    @mock.patch('esgfpid.coupling.Coupler.retrieve_datasethandles_or_versionnumbers_of_allversions')
    def test_unpublish_all_versions_nosolr__butconsumersolr_ok(self, solr_asker_patch):

        # Patch coupler
        mydict = dict(dataset_handles=None, version_numbers=None)
        solr_asker_patch.return_value = mydict

        # Make test connector
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        data_node = TESTVALUES['data_node']
        testconnector = esgfpid.Connector(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_urls = TESTVALUES['url_messaging_service'],
            messaging_service_exchange_name = TESTVALUES['messaging_exchange'],
            data_node = TESTVALUES['data_node'],
            solr_url = TESTVALUES['solr_url'],
            messaging_service_username = TESTVALUES['rabbit_username'],
            messaging_service_password = TESTVALUES['rabbit_password'],
            consumer_solr_url="fake_solr_whatever"
        )
        self.__patch_connector_with_rabbit_mock(testconnector)

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        data_node = TESTVALUES['data_node']

        # Run code to be tested:
        testconnector.unpublish_all_versions(
            drs_id=drs_id,
            data_node=data_node)

        # Check result:
        expected_rabbit_task = {
            "operation": "unpublish_all_versions",
            "message_timestamp": "anydate",
            "data_node": TESTVALUES['data_node'],
            "aggregation_level":"dataset",
            "drs_id":drs_id,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all',
            "consumer_solr_url":"fake_solr_whatever"
        }
        
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    #
    # Errata
    #

    def test_add_errata_id_several_ok(self):

        # Test variables:
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        errata_ids = TESTVALUES['errata_ids']

        # Run code to be tested:
        self.default_testconnector.add_errata_ids(
            drs_id=drs_id,
            version_number=version_number,
            errata_ids=errata_ids
        )

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+TESTVALUES['prefix']+"/afd65cd0-9296-35bc-a706-be98665c9c36",
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":errata_ids,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":drs_id,
            "version_number":version_number
        }

        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    def test_add_errata_id_one_ok(self):

        # Test variables:
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        errata_id = TESTVALUES['errata_id']

        # Run code to be tested:
        self.default_testconnector.add_errata_ids(
            drs_id=drs_id,
            version_number=version_number,
            errata_ids=errata_id
        )

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+TESTVALUES['prefix']+"/afd65cd0-9296-35bc-a706-be98665c9c36",
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":[errata_id],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":drs_id,
            "version_number":version_number
        }

        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    #
    # Shopping Cart
    #

    def test_make_shopping_cart_pid(self):

        # Test variables
        prefix = TESTVALUES['prefix']
        content1 = ['foo', 'hdl:bar', 'hdl:BAZ']
        content2 = ['baz', 'bar', 'foo']

        # Run code to be tested:
        pid1 = self.default_testconnector.create_shopping_cart_pid(content1)
        received_rabbit_task1 = self.default_rabbitmock.send_message_to_queue.call_args[0][0]
        pid2 = self.default_testconnector.create_shopping_cart_pid(content2)
        received_rabbit_task2 = self.default_rabbitmock.send_message_to_queue.call_args[0][0]

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
        
        tests.utils.replace_date_with_string(received_rabbit_task1)
        tests.utils.replace_date_with_string(received_rabbit_task2)
        same1 = tests.utils.is_json_same(expected_rabbit_task1, received_rabbit_task1)
        same2 = tests.utils.is_json_same(expected_rabbit_task2, received_rabbit_task2)
        self.assertTrue(same1, error_message(expected_rabbit_task1, received_rabbit_task1))
        self.assertTrue(same2, error_message(expected_rabbit_task2, received_rabbit_task2))
        self.assertTrue(pid1==pid2, 'Both pids are not the same.')

    #
    # Threads
    #

    def test_start_messaging_thread(self):
        LOGGER.debug('Thread test')
        self.default_testconnector.start_messaging_thread()
        self.default_rabbitmock.start.assert_called_with()

    def test_finish_messaging_thread(self):
        LOGGER.debug('Thread test')
        self.default_testconnector.finish_messaging_thread()
        self.default_rabbitmock.finish.assert_called_with()

    def test_force_finish_messaging_thread(self):
        LOGGER.debug('Thread test')
        self.default_testconnector.force_finish_messaging_thread()
        self.default_rabbitmock.force_finish.assert_called_with()

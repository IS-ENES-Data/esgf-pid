import unittest
import mock
import logging
import json
import sys
from tests.utils import compare_json_return_errormessage as error_message
import tests.utils

import esgfpid.assistant.unpublish
from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

class UnpublicationTestCase(unittest.TestCase):

    def tearDown(self):
        LOGGER.info('#############################')

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def test_unpublish_one_version_by_version_number(self):

        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        args = TESTHELPERS.get_args_for_unpublish_one()
        assistant = esgfpid.assistant.unpublish.AssistantOneVersion(coupler=testcoupler, **args)
  
        # Run code to be tested:
        assistant.unpublish_one_dataset_version(
            version_number=DS_VERSION
        )

        # Check result:
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_unpub_one()
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_unpublish_one_version_by_version_number_and_handle(self):
 
        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        args = TESTHELPERS.get_args_for_unpublish_one()
        assistant = esgfpid.assistant.unpublish.AssistantOneVersion(coupler=testcoupler, **args)

        # Run code to be tested:
        assistant.unpublish_one_dataset_version(
            version_number=DS_VERSION,
            dataset_handle=DATASETHANDLE_HDL # is redundant, but will be checked.
        )

        # Check result:
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_unpub_one()
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_unpublish_one_version_wrong_handle(self):

        # Test variables
        version_number = '999888'
        wrong_handle = PREFIX_NO_HDL+'/miauz'
  
        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        args = TESTHELPERS.get_args_for_unpublish_one()
        assistant = esgfpid.assistant.unpublish.AssistantOneVersion(coupler=testcoupler, **args)

        # Run code to be tested and check exception:
        with self.assertRaises(ValueError):
            assistant.unpublish_one_dataset_version(
                version_number=version_number,
                dataset_handle=wrong_handle)

    def test_unpublish_one_version_version_is_none(self):
        
        # Test variables
        version_number = None

        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        args = TESTHELPERS.get_args_for_unpublish_one()
        assistant = esgfpid.assistant.unpublish.AssistantOneVersion(coupler=testcoupler, **args)

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant.unpublish_one_dataset_version(version_number=version_number)

    def test_unpublish_one_version_no_version_given(self):
        
        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        args = TESTHELPERS.get_args_for_unpublish_one()
        assistant = esgfpid.assistant.unpublish.AssistantOneVersion(coupler=testcoupler, **args)

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant.unpublish_one_dataset_version()

    def test_unpublish_all_versions_nosolr_consumer_must_find_versions_ok(self):        

        # Preparations:
        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        args = TESTHELPERS.get_args_for_unpublish_all()
        args['consumer_solr_url']=SOLR_URL_CONSUMER
        assistant = esgfpid.assistant.unpublish.AssistantAllVersions(coupler=testcoupler, **args)

        # Run code to be tested:
        assistant.unpublish_all_dataset_versions()

        # Check result:
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_unpub_all()
        expected_rabbit_task["consumer_solr_url"] = SOLR_URL_CONSUMER
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_unpublish_all_versions_solr_off_consumer_must_find_versions_ok(self):        

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_url=None, solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        args = TESTHELPERS.get_args_for_unpublish_all()
        args["consumer_solr_url"]=SOLR_URL_CONSUMER
        assistant = esgfpid.assistant.unpublish.AssistantAllVersions(coupler=testcoupler, **args)

        # Run code to be tested:
        assistant.unpublish_all_dataset_versions()

        # Check result:
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_unpub_all()
        expected_rabbit_task["consumer_solr_url"] = SOLR_URL_CONSUMER
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_unpublish_all_versions_by_handles_ok(self):

        # Test variables
        list_of_dataset_handles = [PREFIX_NO_HDL+'/bla', PREFIX_NO_HDL+'/blub']

        # Set solr mock to return handles:
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_list_of_datasets_and_versions(testcoupler, list_of_dataset_handles, None)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        args = TESTHELPERS.get_args_for_unpublish_all()
        assistant = esgfpid.assistant.unpublish.AssistantAllVersions(coupler=testcoupler, **args)

        # Run code to be tested:
        assistant.unpublish_all_dataset_versions()

        # Check result:
        received_rabbit_task1 = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 0)
        received_rabbit_task2 = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 1)
        expected_rabbit_task1 = TESTHELPERS.get_rabbit_message_unpub_one()
        expected_rabbit_task1["handle"]=list_of_dataset_handles[0]
        del expected_rabbit_task1["version_number"]
        expected_rabbit_task2 = TESTHELPERS.get_rabbit_message_unpub_one()
        expected_rabbit_task2["handle"]=list_of_dataset_handles[1]
        del expected_rabbit_task2["version_number"]
        same1 = utils.is_json_same(expected_rabbit_task1, received_rabbit_task1)
        same2 = utils.is_json_same(expected_rabbit_task2, received_rabbit_task2)
        self.assertTrue(same1, error_message(expected_rabbit_task1, received_rabbit_task1))
        self.assertTrue(same2, error_message(expected_rabbit_task2, received_rabbit_task2))

    def test_unpublish_all_versions_by_version_numbers_ok(self):
        
        # Test variables
        list_of_version_numbers = [DS_VERSION, DS_VERSION2]

        # Set solr mock to return handles:
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_list_of_datasets_and_versions(testcoupler, None, list_of_version_numbers)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        args = TESTHELPERS.get_args_for_unpublish_all()
        assistant = esgfpid.assistant.unpublish.AssistantAllVersions(coupler=testcoupler, **args)
        #self.coupler._Coupler__solr_sender.datasethandles_or_versionnumbers_of_allversions['version_numbers'] = list_of_version_numbers

        # Run code to be tested:
        assistant.unpublish_all_dataset_versions()

        # Check result:
        expected_rabbit_task1 = {
            "operation": "unpublish_one_version",
            "aggregation_level": "dataset",
            "message_timestamp": "anydate",
            "data_node": DATA_NODE,
            "handle": DATASETHANDLE_HDL,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id": DRS_ID,
            "version_number": DS_VERSION
        }
        expected_rabbit_task2 = {
            "operation": "unpublish_one_version",
            "aggregation_level": "dataset",
            "message_timestamp": "anydate",
            "data_node": DATA_NODE,
            "handle": DATASETHANDLE_HDL2,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id": DRS_ID,
            "version_number": DS_VERSION2
        }
        received_rabbit_task1 = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 0)
        received_rabbit_task2 = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 1)
        same1 = utils.is_json_same(expected_rabbit_task1, received_rabbit_task1)
        same2 = utils.is_json_same(expected_rabbit_task2, received_rabbit_task2)
        self.assertTrue(same1, error_message(expected_rabbit_task1, received_rabbit_task1))
        self.assertTrue(same2, error_message(expected_rabbit_task2, received_rabbit_task2))
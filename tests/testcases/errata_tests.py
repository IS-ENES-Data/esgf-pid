import unittest
import mock
import logging
import json
import tests.utils as utils
from tests.utils import compare_json_return_errormessage as error_message

from esgfpid.assistant.errata import ErrataAssistant
from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS


# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

'''
Unit tests for esgfpid.assistant.errata.

This module needs a coupler to work: It forwards calls to
RabbitMQ to the coupler.

For the tests, we use a real coupler object that has a
mocked RabbitMQ connection, and whose solr communication
is simply switched off (not mocked), as it is never used
for errata operations anyway.
'''
class ErrataTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def test_init_ok(self):

        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
  
        # Run code to be tested:
        assistant = ErrataAssistant(
            prefix=PREFIX_NO_HDL,
            coupler=testcoupler
        )

        # Check result:
        self.assertIsInstance(assistant, ErrataAssistant, 'Constructor fail.')

    def test_add_errata_id_one_as_string(self):

        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        assistant = ErrataAssistant(
            prefix=PREFIX_NO_HDL,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.add_errata_ids(
            drs_id=DRS_ID,
            version_number=DS_VERSION,
            errata_ids=ERRATA
        )

        # Check result:
        expected_rabbit_task = {
            "handle":DATASETHANDLE_HDL,
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":[ERRATA],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":DRS_ID,
            "version_number":DS_VERSION
        }
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_errata_id_one_as_list(self):

        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        assistant = ErrataAssistant(
            prefix=PREFIX_NO_HDL,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.add_errata_ids(
            drs_id=DRS_ID,
            version_number=DS_VERSION,
            errata_ids=[ERRATA]
        )

        # Check result:
        expected_rabbit_task = {
            "handle":DATASETHANDLE_HDL,
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":[ERRATA],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":DRS_ID,
            "version_number":DS_VERSION
        }
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_errata_id_several(self):

        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        assistant = ErrataAssistant(
            prefix=PREFIX_NO_HDL,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.add_errata_ids(
            drs_id=DRS_ID,
            version_number=DS_VERSION,
            errata_ids=ERRATA_SEVERAL
        )

        # Check result:
        expected_rabbit_task = {
            "handle":DATASETHANDLE_HDL,
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":ERRATA_SEVERAL,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":DRS_ID,
            "version_number":DS_VERSION
        }
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_errata_id_missing_args(self):

        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        assistant = ErrataAssistant(
            prefix=PREFIX_NO_HDL,
            coupler=testcoupler
        )

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant.add_errata_ids(
                drs_id=DRS_ID,
                version_number=DS_VERSION
            )

    def test_remove_errata_id_several(self):

        # Preparations
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        assistant = ErrataAssistant(
            prefix=PREFIX_NO_HDL,
            coupler=testcoupler
        )

        # Run code to be tested:
        assistant.remove_errata_ids(
            drs_id=DRS_ID,
            version_number=DS_VERSION,
            errata_ids=ERRATA_SEVERAL
        )

        # Check result:
        expected_rabbit_task = {
            "handle":DATASETHANDLE_HDL,
            "operation": "remove_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":ERRATA_SEVERAL,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.remove',
            "drs_id":DRS_ID,
            "version_number":DS_VERSION
        }
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler)
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))
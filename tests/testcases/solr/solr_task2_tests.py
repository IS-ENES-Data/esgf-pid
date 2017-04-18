import unittest
import mock
import logging
import json
import esgfpid.solr.solr
import esgfpid.solr.tasks.all_versions_of_dataset as task
import tests.mocks.responsemock
import tests.utils

# Logging:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

# Load some data that is needed for testing
PATH_RES = tests.utils.get_super_neighbour_directory(__file__, 'resources')
SOLR_RESPONSE = json.load(open(PATH_RES+'/solr_response.json'))


QUERY = {'format': 'application/solr+json', 'facets': 'pid,version', 'limit': 0, 'distrib': False, 'drs_id':'abc', 'type': 'Dataset'}


class SolrTask2TestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def make_testtask(self):
        testsolr = TESTHELPERS.get_testsolr()
        testtask = task.FindVersionsOfSameDataset(testsolr)
        return testtask

    def get_args_dict(self):
        return dict(
            drs_id = 'abc',
            version_number = '2016',
            data_node = 'foo.de',
            prefix = '123')

    def fake_solr_response(self, pid, vers):
        resp = {
            "facet_counts": {
                "facet_fields": {
                    "pid": pid,
                    "version": vers
                }
            }
        }
        return resp

    # Actual tests:

    def test_init_ok(self):

        # Preparations
        testsolr = TESTHELPERS.get_testsolr()

        # Run code to be tested:
        testtask = task.FindVersionsOfSameDataset(testsolr)

        # Check result
        self.assertIsInstance(testtask, task.FindVersionsOfSameDataset, 'Constructor fail.')

    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_dataset_handles_both_returned_ok(self, getpatch):

        # Define the replacement for the patched method:
        pids = ["123/456",3,"123/234",1,"987/567",2]
        versions = ["2016",3,"2015",7,"2099",8]
        getpatch.return_value = self.fake_solr_response(pids, versions)

        # Preparations
        task = self.make_testtask()

        # Test variables:
        drs_id = 'abc'
        prefix = '123'

        # Run code to be tested:
        received_dict = task.retrieve_dataset_handles_or_version_numbers_of_all_versions(drs_id, prefix)

        # Check result:
        # Was the correct query sent?
        expected_query = QUERY
        getpatch.assert_called_once_with(expected_query)
        # Was the response treated correctly?
        expected_dict = {
            'version_numbers': ['2015', '2016', '2099'],
            'dataset_handles': ['hdl:123/987/567', 'hdl:123/234', 'hdl:123/456']
        }
        self.assertEqual(expected_dict, received_dict, 'Expected %s, but got %s' % (expected_dict, received_dict))

    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_dataset_handles_only_handles_returned_ok(self, getpatch):

        # Define the replacement for the patched method:
        pids = ["123/456",3,"123/234",1,"987/567",2]
        versions = []
        getpatch.return_value = self.fake_solr_response(pids, versions)

        # Preparations
        task = self.make_testtask()

        # Test variables:
        drs_id = 'abc'
        prefix = '123'

        # Run code to be tested:
        received_dict = task.retrieve_dataset_handles_or_version_numbers_of_all_versions(drs_id, prefix)

        # Check result:
        # Was the correct query sent?
        expected_query = QUERY
        getpatch.assert_called_once_with(expected_query)
        # Was the response treated correctly?
        expected_dict = {
            'version_numbers': [],
            'dataset_handles': ['hdl:123/987/567', 'hdl:123/234', 'hdl:123/456']
        }
        self.assertEqual(expected_dict, received_dict, 'Expected %s, but got %s' % (expected_dict, received_dict))



    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_dataset_handles_only_versions_returned_ok(self, getpatch):

        # Define the replacement for the patched method:
        pids = []
        versions = ["2016",3,"2015",7,"2099",8]
        getpatch.return_value = self.fake_solr_response(pids, versions)

        # Preparations
        task = self.make_testtask()

        # Test variables:
        drs_id = 'abc'
        prefix = '123'

        # Run code to be tested:
        received_dict = task.retrieve_dataset_handles_or_version_numbers_of_all_versions(drs_id, prefix)

        # Check result:
        # Was the correct query sent?
        expected_query = QUERY
        getpatch.assert_called_once_with(expected_query)
        # Was the response treated correctly?
        expected_dict = {
            'version_numbers': ['2015', '2016', '2099'],
            'dataset_handles': []
        }
        self.assertEqual(expected_dict, received_dict, 'Expected %s, but got %s' % (expected_dict, received_dict))



    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_dataset_handles_emtpy_lists_returned_ok(self, getpatch):

        # Define the replacement for the patched method:
        pids = []
        versions = []
        getpatch.return_value = self.fake_solr_response(pids, versions)

        # Preparations
        task = self.make_testtask()

        # Test variables:
        drs_id = 'abc'
        prefix = '123'

        # Run code to be tested:
        received_dict = task.retrieve_dataset_handles_or_version_numbers_of_all_versions(drs_id, prefix)

        # Check result:
        # Was the correct query sent?
        expected_query = QUERY
        getpatch.assert_called_once_with(expected_query)
        # Was the response treated correctly?
        expected_dict = {
            'version_numbers': [],
            'dataset_handles': []
        }
        self.assertEqual(expected_dict, received_dict, 'Expected %s, but got %s' % (expected_dict, received_dict))


    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_dataset_handles_solr_error(self, getpatch):

        # Define the replacement for the patched method:
        getpatch.side_effect = esgfpid.exceptions.SolrError()

        # Preparations
        task = self.make_testtask()

        # Test variables:
        drs_id = 'abc'
        prefix = '123'

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.SolrError):
            received_dict = task.retrieve_dataset_handles_or_version_numbers_of_all_versions(drs_id, prefix)




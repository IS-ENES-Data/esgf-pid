import unittest
import mock
import logging
import json
import esgfpid.solr.solr
import esgfpid.solr.tasks.filehandles_same_dataset as task
import tests.mocks.responsemock
import tests.utils
import tests.resources

# Logging:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS

# Load some data that is needed for testing
PATH_RES = tests.utils.get_super_neighbour_directory(__file__, 'resources')
SOLR_RESPONSE = json.load(open(PATH_RES+'/solr_response.json'))

QUERY1 = {'format': 'application/solr+json', 'facets': 'tracking_id', 'limit': 0, 'distrib': False, 'dataset_id':'abc.v2016|foo.de', 'type': 'File'}
QUERY2 = {'format': 'application/solr+json', 'facets': 'tracking_id', 'limit': 0, 'distrib': False, 'query': 'dataset_id:abc.v2016|*', 'type': 'File'}


class SolrTask1TestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def make_testtask(self):
        testsolr = TESTHELPERS.get_testsolr()
        testtask = task.FindFilesOfSameDatasetVersion(testsolr)
        return testtask

    def get_args_dict(self):
        return dict(
            drs_id = 'abc',
            version_number = '2016',
            data_node = 'foo.de',
            prefix = '123')

    def fake_solr_response(self, ids):
        resp = {
            "facet_counts": {
                "facet_fields": {
                    "tracking_id": ids
                }
            }
        }
        return resp

    # Actual tests:

    def test_init_ok(self):

        # Preparations
        testsolr = TESTHELPERS.get_testsolr()

        # Run code to be tested:
        testtask = task.FindFilesOfSameDatasetVersion(testsolr)

        # Check result
        self.assertIsInstance(testtask, task.FindFilesOfSameDatasetVersion, 'Constructor fail.')

    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_file_handles_of_same_dataset_A_ok_patched(self, getpatch):
        '''
        In this test, only strategy 1 is used.
        serverconnector.send_query returns three handles on the first call.
        '''

        # Define the replacement for the patched method:
        handles = ["123/456",3,"123/234",1,"987/567",2]
        getpatch.return_value = self.fake_solr_response(handles)

        # Preparations
        task = self.make_testtask()

        # Test variables:
        args = self.get_args_dict()

        # received_handles code to be tested:
        received_handles = task.retrieve_file_handles_of_same_dataset(**args)

        # Check result:
        # Was the correct query sent?
        #expected_query = {'format': 'application/solr+json', 'facets': 'handle,tracking_id', 'limit': 0, 'distrib': False, 'dataset_id': 'abc.v2016|foo.de', 'type': 'File'}
        expected_query = QUERY1
        getpatch.assert_called_once_with(expected_query)
        # Was the response treated correctly?
        expected_handles = ['hdl:123/987/567', 'hdl:123/234', 'hdl:123/456']
        self.assertEqual(expected_handles, received_handles, 'Expected %s, but got %s' % (expected_handles, received_handles))


    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_file_handles_of_same_dataset_AB_nohandles_patched(self, getpatch):
        '''
        In this test, both strategies are used.
        serverconnector.send_query returns [] on the first call,
        so the second call is issued, but this also returns [].
        '''

        # Define the replacement for the patched method:
        getpatch.return_value = self.fake_solr_response([])

        # Preparations
        task = self.make_testtask()

        # Test variables:
        args = self.get_args_dict()

        # Run code to be tested:
        received_handles = task.retrieve_file_handles_of_same_dataset(**args)

        # Check result:
        # Was the correct query sent?
        expected_query_1 = QUERY1
        expected_query_2 = QUERY2
        getpatch.assert_any_call(expected_query_1)
        getpatch.assert_called_with(expected_query_2)
        # Was the response treated correctly?
        self.assertEqual(received_handles, [], 'Expected empty list, but got: '+str(received_handles))

    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_file_handles_of_same_dataset_A_nohandle_B_ok_patched(self, getpatch):
        '''
        In this test, both strategies are used.
        serverconnector.send_query returns [] on the first call,
        so the second call is issued, but this also returns [].
        '''

        # Test variables:
        args = self.get_args_dict()
        handles = ["123/456",3,"123/234",1,"987/567",2]
        
        # Define the replacement for the patched method:
        def different_mock_response_depending_on_query(query):
            if query == QUERY1:
                return self.fake_solr_response([])
            elif query == QUERY2:
                return self.fake_solr_response(handles)
            else:
                raise ValueError('Something went wrong with the test. Wrong query: '+str(query))        
        getpatch.side_effect = different_mock_response_depending_on_query

        # Preparations
        task = self.make_testtask()

        # Run code to be tested:
        received_handles = task.retrieve_file_handles_of_same_dataset(**args)

        # Check result:
        # Was the correct query sent?
        expected_query_1 = QUERY1
        expected_query_2 = QUERY2
        getpatch.assert_any_call(expected_query_1)
        getpatch.assert_called_with(expected_query_2)
        # Was the response treated correctly?
        expected_handles = ['hdl:123/987/567', 'hdl:123/234', 'hdl:123/456']
        self.assertEqual(expected_handles, received_handles, 'Expected %s, but got %s' % (expected_handles, received_handles))

    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_file_handles_of_same_dataset_A_error_B_ok_patched(self, getpatch):
        '''
        In this test, both strategies are used.
        serverconnector.send_query returns [] on the first call,
        so the second call is issued, but this also returns [].
        '''

        # Test variables:
        args = self.get_args_dict()
        handles = ["123/456",3,"123/234",1,"987/567",2]
        
        # Define the replacement for the patched method:
        def different_mock_response_depending_on_query(query):
            if query == QUERY1:
                raise esgfpid.exceptions.SolrError('Whatever...')
            elif query == QUERY2:
                return self.fake_solr_response(handles)
            else:
                raise ValueError('Something went wrong with the test. Wrong query: '+str(query))        
        getpatch.side_effect = different_mock_response_depending_on_query

        # Preparations
        task = self.make_testtask()

        # Run code to be tested:
        received_handles = task.retrieve_file_handles_of_same_dataset(**args)

        # Check result:
        # Was the correct query sent?
        expected_query_1 = QUERY1
        expected_query_2 = QUERY2
        getpatch.assert_any_call(expected_query_1)
        getpatch.assert_called_with(expected_query_2)
        # Was the response treated correctly?
        expected_handles = ['hdl:123/987/567', 'hdl:123/234', 'hdl:123/456']
        self.assertEqual(expected_handles, received_handles, 'Expected %s, but got %s' % (expected_handles, received_handles))

    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_file_handles_of_same_dataset_A_error_B_nohandles_patched(self, getpatch):
        '''
        In this test, both strategies are used.
        serverconnector.send_query returns [] on the first call,
        so the second call is issued, but this also returns [].
        '''

        # Test variables:
        args = self.get_args_dict()
        
        # Define the replacement for the patched method:
        def different_mock_response_depending_on_query(query):
            if query == QUERY1:
                raise esgfpid.exceptions.SolrError('Whatever...')
            elif query == QUERY2:
                return self.fake_solr_response([])
            else:
                raise ValueError('Something went wrong with the test. Wrong query: '+str(query))        
        getpatch.side_effect = different_mock_response_depending_on_query

        # Preparations
        task = self.make_testtask()

        # Run code to be tested:
        received_handles = task.retrieve_file_handles_of_same_dataset(**args)

        # Check result:
        # Was the correct query sent?
        expected_query_1 = QUERY1
        expected_query_2 = QUERY2
        getpatch.assert_any_call(expected_query_1)
        getpatch.assert_called_with(expected_query_2)
        # Was the response treated correctly?
        self.assertEqual([], received_handles, 'Expected empty list, but got %s' % received_handles)

    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_file_handles_of_same_dataset_A_error_B_error_patched(self, getpatch):
        '''
        In this test, both strategies are used.
        serverconnector.send_query returns [] on the first call,
        so the second call is issued, but this also returns [].
        '''

        # Test variables:
        args = self.get_args_dict()
        
        # Define the replacement for the patched method:
        def different_mock_response_depending_on_query(query):
            if query == QUERY1:
                raise esgfpid.exceptions.SolrError('Whatever 1...')
            elif query == QUERY2:
                raise esgfpid.exceptions.SolrError('Whatever 2...')
            else:
                raise ValueError('Something went wrong with the test. Wrong query: '+str(query))        
        getpatch.side_effect = different_mock_response_depending_on_query

        # Preparations
        task = self.make_testtask()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            received_handles = task.retrieve_file_handles_of_same_dataset(**args)
        self.assertIn('Failure in both queries', raised.exception.message)
        self.assertIn('Whatever 1', raised.exception.message)
        self.assertIn('Whatever 2', raised.exception.message)


    @mock.patch('esgfpid.solr.serverconnector.SolrServerConnector.send_query')
    def test_retrieve_file_handles_of_same_dataset_A_nohandle_B_error_patched(self, getpatch):
        '''
        In this test, both strategies are used.
        serverconnector.send_query returns [] on the first call,
        so the second call is issued, but this also returns [].
        '''

        # Test variables:
        args = self.get_args_dict()
        
        # Define the replacement for the patched method:
        def different_mock_response_depending_on_query(query):
            if query == QUERY1:
                return self.fake_solr_response([])
            elif query == QUERY2:
                raise esgfpid.exceptions.SolrError('Whatever 2...')
            else:
                raise ValueError('Something went wrong with the test. Wrong query: '+str(query))        
        getpatch.side_effect = different_mock_response_depending_on_query

        # Preparations
        task = self.make_testtask()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            received_handles = task.retrieve_file_handles_of_same_dataset(**args)
        self.assertIn('Failure in both queries', raised.exception.message)
        self.assertIn('First query returned an empty list', raised.exception.message)
        self.assertIn('Whatever 2', raised.exception.message)


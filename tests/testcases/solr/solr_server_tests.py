import unittest
import mock
import logging
import json
import requests
import esgfpid.solr.solr
import tests.mocks.responsemock


# Logging:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Load some data that is needed for testing
PATH_RES = tests.utils.get_super_neighbour_directory(__file__, 'resources')
SOLR_RESPONSE = json.load(open(PATH_RES+'/solr_response.json'))

# Test resources:
import resources.TESTVALUES as TESTHELPERS

'''
Unit tests for module esgfpid.solr.serverconnector.

This module has no references to other modules, so
no other modules need to be used or mocked.

However, it talks to a solr server (via requests
module), so we need to mock that one. We patch all
calls to requests.get()
'''
class SolrServerConnectorTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def test_init_ok(self):

        # Run code to be tested:
        testsolr = esgfpid.solr.serverconnector.SolrServerConnector(
            solr_url = 'blablabla///',
            https_verify = False,
            disable_insecure_request_warning = False
        )

        # Check result
        self.assertIsInstance(testsolr, esgfpid.solr.serverconnector.SolrServerConnector, 'Constructor fail.')
        self.assertFalse(testsolr._SolrServerConnector__https_verify, 'HTTPS verify not set to False.')
        self.assertEquals(testsolr._SolrServerConnector__solr_url, 'blablabla', 'Wrong url.')


    def test_init_with_optional_args(self):

        # Run code to be tested:
        testsolr = esgfpid.solr.serverconnector.SolrServerConnector(
            solr_url = 'blablabla',
            https_verify = True,
            disable_insecure_request_warning = True
        )

        self.assertIsInstance(testsolr, esgfpid.solr.serverconnector.SolrServerConnector, 'Constructor fail.')
        self.assertTrue(testsolr._SolrServerConnector__https_verify, 'HTTPS verify not set to True.')

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_send_query_ok_patched(self, getpatch):

        # Define the replacement for the patched method:
        mock_response = tests.mocks.responsemock.MockSolrResponse(success=True)
        getpatch.return_value = mock_response

        # Test variables
        testsolr = TESTHELPERS.get_testsolr_connector()

        # Run code to be tested:
        response = testsolr.send_query('blah')

        # Check result:
        # We expect the mock response to be returned and parsed to JSON.
        self.assertTrue('responseHeader' in response, 'Solr response was not correct JSON: '+str(response))
        self.assertTrue(len(response['facet_counts']['facet_fields']['bla'])==4, 'JSON content was not transmitted correctly.')

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_send_query_200_empty_patched(self, getpatch):

        # Define the replacement for the patched method:
        mock_response = tests.mocks.responsemock.MockSolrResponse(success=True, content='NONE')
        getpatch.return_value = mock_response

        # Test variables
        testsolr = TESTHELPERS.get_testsolr_connector()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query('blah')
        self.assertIn('empty response', raised.exception.message)

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_send_query_broken_json_patched(self, getpatch):

        # Define the replacement for the patched method:
        broken_json = '{"responseHeader":{}, "response":{}, "facet_counts": {}'
        mock_response = tests.mocks.responsemock.MockSolrResponse(success=True, content=broken_json)
        getpatch.return_value = mock_response

        # Test variables
        testsolr = TESTHELPERS.get_testsolr_connector()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query('blah')
        self.assertIn('no valid JSON', raised.exception.message)

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_send_query_return_none_patched(self, getpatch):

        # Define the replacement for the patched method:
        getpatch.return_value = None

        # Test variables
        testsolr = TESTHELPERS.get_testsolr_connector()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query('blah')
        self.assertIn('no response', raised.exception.message)

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_send_query_http_404_patched(self, getpatch):

        # Define the replacement for the patched method:
        mock_response = tests.mocks.responsemock.MockSolrResponse(notfound=True)
        getpatch.return_value = mock_response

        # Test variables
        testsolr = TESTHELPERS.get_testsolr_connector()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query('blah')
        self.assertIn('HTTP 404', raised.exception.message)

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_send_query_patched_connection_error(self, getpatch):

        # Define the replacement for the patched method:
        getpatch.side_effect = requests.exceptions.ConnectionError()

        # Test variables
        testsolr = TESTHELPERS.get_testsolr_connector()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query('blah')
        self.assertIn('ConnectionError', raised.exception.message)

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_send_query_patched_5000(self, getpatch):

        # Define the replacement for the patched method:
        mock_response = tests.mocks.responsemock.MockSolrResponse(status_code=5000)
        getpatch.return_value = mock_response

        # Test variables
        testsolr = TESTHELPERS.get_testsolr_connector()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query('blah')
        self.assertIn('code 5000', raised.exception.message)

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

# Test resources:
from tests.resources.IGNORE.TESTVALUES_IGNORE import TESTVALUES_SOLR
SOLR_URL = 'http://'+TESTVALUES_SOLR['solr_url']
PREFIX = TESTVALUES_SOLR['prefix']
DRS_ID = TESTVALUES_SOLR['drs_id']
VERSION_NUMBER =  TESTVALUES_SOLR['version_number']
DATA_NODE = TESTVALUES_SOLR['data_node']
FILES = TESTVALUES_SOLR['files']

class SolrIntegrationTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')

    def tearDown(self):
        LOGGER.info('#############################')

    def make_testsolr_with_access(self):
        testsolr = esgfpid.solr.solr.SolrInteractor(
            solr_url = SOLR_URL,
            prefix = PREFIX
        )
        return testsolr

    # Actual tests:

    def test_send_query_ok(self):
        '''
        Sending basic query that returns number of all files found.'''

        # Test variables
        testsolr = self.make_testsolr_with_access()
        query = testsolr.make_solr_base_query()

        # Run code to be tested:
        response = testsolr.send_query(query)

        # Check result:
        # We expect the mock response to be returned and parsed to JSON.
        self.assertTrue('responseHeader' in response, 'Solr response to query %s had no responseHeader: %s' % (query, response))
        self.assertTrue('facet_counts' in response, 'Solr response to query %s had no facet_counts: %s' % (query, response))
        self.assertTrue('response' in response, 'Solr response to query %s had no response: %s' % (query, response))


    def test_send_query_connection_error(self):

        # Test variables
        testsolr = esgfpid.solr.solr.SolrInteractor(
            solr_url = 'https://ihopethisdoesnotexist.de',
            prefix = PREFIX
        )
        query = 'doesntmatter'

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query(query)
        self.assertIn('ConnectionError', raised.exception.message)

    def test_send_query_404(self):

        # Test variables
        testsolr = esgfpid.solr.solr.SolrInteractor(
            solr_url = SOLR_URL+'/hahahah/hohohoh/',
            prefix = PREFIX
        )
        query = 'doesntmatter'

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query(query)
        self.assertIn('HTTP 404', raised.exception.message)


    def test_send_query_400(self):

        # Test variables
        testsolr = esgfpid.solr.solr.SolrInteractor(
            solr_url = SOLR_URL,
            prefix = PREFIX
        )
        query = 'foo'

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query(query)
        self.assertIn('Solr replied with code 400', raised.exception.message)

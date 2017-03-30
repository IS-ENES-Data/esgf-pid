import unittest
import mock
import logging
import json
import requests
import esgfpid.solr.solr

# Logging:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from tests.resources.IGNORE.TESTVALUES_IGNORE import TESTVALUES_SOLR
SOLR_URL_HTTP = 'http://'+TESTVALUES_SOLR['solr_url']
SOLR_URL_HTTPS = 'https://'+TESTVALUES_SOLR['solr_url']
PREFIX = TESTVALUES_SOLR['prefix']

class SolrServerIntegrationTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')

    def tearDown(self):
        LOGGER.info('#############################')

    # Actual tests:

    def test_send_query_ok(self):

        # Test variables
        testconn = esgfpid.solr.serverconnector.SolrServerConnector(solr_url = SOLR_URL_HTTP)
        query = dict(
            distrib=False,
            type='File',
            format='application/solr+json',
            limit=0
        )

        # Run code to be tested:
        response = testconn.send_query(query)

        # Check result:
        self.assertTrue('responseHeader' in response, 'Solr response was not correct JSON: '+str(response))

    def test_send_query_ok_warnings_1_are_enabled(self):
        '''
        IMPORTANT:
        This only works if this test is run before the disabled test.
        It seems that the disabling stays...
        That is why there is numbers in the test name - they
        seem to be run in alphabetical order.
        '''

        LOGGER.debug('*** Without disabling ***')

        # Test variables
        testconn = esgfpid.solr.serverconnector.SolrServerConnector(
            solr_url = SOLR_URL_HTTPS,
            disable_insecure_request_warning=False,
            https_verify=False
        )
        query = dict(
            distrib=False,
            type='File',
            format='application/solr+json',
            limit=0
        )

        # Capture stderr:
        import sys
        from StringIO import StringIO
        saved_stdout = sys.stderr
        out = StringIO()
        sys.stderr = out
        # Run code to be tested:
        response = testconn.send_query(query)
        # Retrieve stderr and reset stderr:
        output = out.getvalue().strip()
        sys.stdout = saved_stdout

        # Check result:
        expected_warning = 'InsecureRequestWarning: Unverified HTTPS request is being made'
        self.assertTrue(expected_warning in output, 'Warning missing: '+output)


    def test_send_query_ok_warnings_2_disabled(self):

        LOGGER.debug('*** With disabling ***')

        # Test variables
        testconn = esgfpid.solr.serverconnector.SolrServerConnector(
            solr_url = SOLR_URL_HTTPS,
            disable_insecure_request_warning=True,
            https_verify=False
        )
        query = dict(
            distrib=False,
            type='File',
            format='application/solr+json',
            limit=0
        )

        # Capture stderr:
        import sys
        from StringIO import StringIO
        saved_stdout = sys.stderr
        out = StringIO()
        sys.stderr = out
        # Run code to be tested:
        response = testconn.send_query(query)
        # Retrieve stderr and reset stderr:
        output = out.getvalue().strip()
        sys.stdout = saved_stdout

        # Check result:
        expected_warning = 'InsecureRequestWarning: Unverified HTTPS request is being made'
        self.assertFalse(expected_warning in output, 'Some stderr occurred, but we tried switching it off: '+output)

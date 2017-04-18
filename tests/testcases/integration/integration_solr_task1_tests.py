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

class SolrTask1IntegrationTestCase(unittest.TestCase):

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

    def test_retrieve_file_handles_of_same_dataset(self):

        # Test variables
        testsolr = self.make_testsolr_with_access()
        drs_id = DRS_ID
        version_number = VERSION_NUMBER
        data_node = DATA_NODE

        # Run code to be tested:
        resp = testsolr.retrieve_file_handles_of_same_dataset(
            drs_id=drs_id,
            version_number=version_number,
            data_node=data_node
        )

        # Check result:
        expected = FILES
        ok = (set(resp) == set(expected))
        self.assertTrue(ok, 'Solr returned:\n'+str(resp)+'\nExpected:\n'+str(expected))

    def test_retrieve_file_handles_of_same_dataset_other_node(self):

        # Test variables
        testsolr = self.make_testsolr_with_access()
        drs_id = DRS_ID
        version_number = VERSION_NUMBER
        data_node = 'bogus.de'

        # Run code to be tested:
        resp = testsolr.retrieve_file_handles_of_same_dataset(
            drs_id=drs_id,
            version_number=version_number,
            data_node=data_node
        )

        # Check result:
        expected = FILES
        ok = (set(resp) == set(expected))
        self.assertTrue(ok, 'Solr returned:\n'+str(resp)+'\nExpected:\n'+str(expected))

    def test_retrieve_file_handles_of_same_dataset_empty_lists(self):

        # Test variables
        testsolr = self.make_testsolr_with_access()
        drs_id = DRS_ID
        version_number = '9999999' # hopefully does not exist, I want an empty list back
        data_node = 'bogus.de'

        # Run code to be tested:
        resp = testsolr.retrieve_file_handles_of_same_dataset(
            drs_id=drs_id,
            version_number=version_number,
            data_node=data_node
        )

        # Check result:
        expected = []
        ok = (set(resp) == set(expected))
        self.assertTrue(ok, 'Solr returned:\n'+str(resp)+'\nExpected:\n'+str(expected))

    def test_retrieve_file_handles_of_same_dataset_errors(self):

        # Test variables
        testsolr = esgfpid.solr.solr.SolrInteractor(
            solr_url = 'https://ihopethisdoesnotexist.de',
            prefix = 'foo'
        )
        drs_id = 'foo'
        version_number = '9999999'
        data_node = 'bogus.de'

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            resp = testsolr.retrieve_file_handles_of_same_dataset(
                drs_id=drs_id,
                version_number=version_number,
                data_node=data_node
            )
        self.assertIn('Failure in both queries.', raised.exception.message)
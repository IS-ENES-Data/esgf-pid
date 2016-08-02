import unittest
import mock
import logging
import json
import requests
import esgfpid.solr.solr
import tests.mocks.responsemock
import tests.resources

# Logging:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Load some data that is needed for testing
PATH_RES = tests.utils.get_neighbour_directory(__file__, 'resources')
SOLR_RESPONSE = json.load(open(PATH_RES+'/solr_response.json'))
from tests.resources.TESTVALUES import TESTVALUES as TESTVALUES

class SolrTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')

    def tearDown(self):
        LOGGER.info('#############################')

    def make_testsolr_without_access(self):
        testsolr = esgfpid.solr.solr.SolrInteractor(
            solr_url = TESTVALUES['solr_url'],
            prefix = TESTVALUES['prefix'],
            switched_off = True
        )
        return testsolr

    def make_testsolr_with_access(self):
        testsolr = esgfpid.solr.solr.SolrInteractor(
            solr_url = TESTVALUES['solr_url'],
            prefix = TESTVALUES['prefix'],
            switched_off = False
        )
        return testsolr

    # Actual tests:

    def test_init_switched_off_ok(self):

        # Run code to be tested:
        testsolr = esgfpid.solr.solr.SolrInteractor(
            solr_url = TESTVALUES['solr_url'],
            prefix = TESTVALUES['prefix'],
            switched_off = True
        )
        has_access = not testsolr.is_switched_off()

        # Check result:
        self.assertIsInstance(testsolr, esgfpid.solr.solr.SolrInteractor, 'Constructor fail.')
        self.assertFalse(has_access, 'Solr pretends to have access although we told it to switch off.')

    def test_init_ok(self):

        # Run code to be tested:
        testsolr = esgfpid.solr.solr.SolrInteractor(
            solr_url = TESTVALUES['solr_url'],
            prefix = TESTVALUES['prefix'],
        )
        has_access = not testsolr.is_switched_off()

        # Check result:
        self.assertIsInstance(testsolr, esgfpid.solr.solr.SolrInteractor, 'Constructor fail.')
        self.assertTrue(has_access, 'Solr pretends not to have access.')

    def test_init_missing_argument(self):

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            testsolr = esgfpid.solr.solr.SolrInteractor(
                solr_url = None,
                prefix = TESTVALUES['prefix'],
            )

    def test_make_solr_base_query_ok(self):

        # Run code to be tested:
        testsolr = self.make_testsolr_without_access()
        query = testsolr.make_solr_base_query()

        # Check result:
        expected = dict(
            distrib=False,
            format='application/solr+json',
            limit=0
        )
        self.assertTrue(query==expected,
            'Make solr base query: Unexpected query made!\nExpected: '+str(expected)+'\nReceived: '+str(query)+'.')

    def test_send_query_switched_off(self):

        # Test variables
        testsolr = self.make_testsolr_without_access()
        query = testsolr.make_solr_base_query()

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.SolrSwitchedOff):
            response = testsolr.send_query(query)

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_send_query_ok_patched(self, getpatch):

        # Define the replacement for the patched method:
        mock_response = tests.mocks.responsemock.MockSolrResponse(success=True)
        getpatch.return_value = mock_response

        # Test variables
        testsolr = self.make_testsolr_with_access()
        query = testsolr.make_solr_base_query()

        # Run code to be tested:
        response = testsolr.send_query(query)

        # Check result:
        # We expect the mock response to be returned and parsed to JSON.
        self.assertTrue('responseHeader' in response, 'Solr response was not correct JSON: '+str(response))
        self.assertTrue(len(response['facet_counts']['facet_fields']['bla'])==4, 'JSON content was not transmitted correctly.')

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_send_query_404_patched(self, getpatch):

        # Define the replacement for the patched method:
        mock_response = tests.mocks.responsemock.MockSolrResponse(notfound=True)
        getpatch.return_value = mock_response

        # Test variables
        testsolr = self.make_testsolr_with_access()
        query = testsolr.make_solr_base_query()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query(query)
        self.assertIn('HTTP 404', raised.exception.message)


    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_send_query_5000_patched(self, getpatch):

        # Define the replacement for the patched method:
        mock_response = tests.mocks.responsemock.MockSolrResponse(status_code=5000)
        getpatch.return_value = mock_response

        # Test variables
        testsolr = self.make_testsolr_with_access()
        query = testsolr.make_solr_base_query()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query(query)
        self.assertIn('code 5000', raised.exception.message)


    def test_retrieve_file_handles_of_same_dataset_no_access(self):

        # Test variables
        testsolr = self.make_testsolr_without_access()
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        data_node = TESTVALUES['data_node']

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrSwitchedOff):
            resp = testsolr.retrieve_file_handles_of_same_dataset(
                drs_id=drs_id,
                version_number=version_number,
                data_node=data_node
            )


    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_retrieve_file_handles_of_same_dataset_patched(self, getpatch):

        # Define the replacement for the patched method:
        prefix = TESTVALUES['prefix']
        response_json = {
            "responseHeader":{},
            "response":{},
            "facet_counts": {
                "facet_fields":{
                    "tracking_id": [
                        prefix+'/123',1,
                        prefix+'/234',1,
                        prefix+'/345',1
                    ]
                }
            }
        }
        mock_response = tests.mocks.responsemock.MockSolrResponse(success=True, content=json.dumps(response_json))
        getpatch.return_value = mock_response

        # Test variables
        testsolr = self.make_testsolr_with_access()
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        data_node = TESTVALUES['data_node']

        # Run code to be tested:
        resp = testsolr.retrieve_file_handles_of_same_dataset(
            drs_id=drs_id,
            version_number=version_number,
            data_node=data_node
        )

        # Check result:
        expected = ['hdl:'+prefix+'/345', 'hdl:'+prefix+'/234', 'hdl:'+prefix+'/123']
        ok = (set(resp) == set(expected))
        self.assertTrue(ok, 'Solr returned:\n'+str(resp)+'\nExpected:\n'+str(expected))

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_send_query_patched_connection_error(self, getpatch):

        # Define the replacement for the patched method:
        getpatch.side_effect = requests.exceptions.ConnectionError()

        # Test variables
        testsolr = self.make_testsolr_with_access()
        query = testsolr.make_solr_base_query()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrError) as raised:
            response = testsolr.send_query(query)
        self.assertIn('ConnectionError', raised.exception.message)

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_retrieve_datasethandles_or_versionnumbers_of_allversions_pids_patched(self, getpatch):

        # Define the replacement for the patched method:
        prefix = TESTVALUES['prefix']
        response_json = {
            "responseHeader":{},
            "response":{},
            "facet_counts": {
                "facet_fields":{
                    "pid": [
                        prefix+'/123',1,
                        prefix+'/234',1,
                        prefix+'/345',1
                    ]
                }
            }
        }
        mock_response = tests.mocks.responsemock.MockSolrResponse(success=True, content=json.dumps(response_json))
        getpatch.return_value = mock_response

        # Test variables
        testsolr = self.make_testsolr_with_access()
        drs_id = TESTVALUES['drs_id1']

        # Run code to be tested:
        resp = testsolr.retrieve_datasethandles_or_versionnumbers_of_allversions(drs_id)

        # Check result:
        expected = {'version_numbers': None, 'dataset_handles': ['hdl:'+prefix+'/345', 'hdl:'+prefix+'/234', 'hdl:'+prefix+'/123']}
        ok = (set(resp) == set(expected))
        self.assertTrue(ok, 'Solr returned:\n'+str(resp)+'\nExpected:\n'+str(expected))

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_retrieve_datasethandles_or_versionnumbers_of_allversions_vers_patched(self, getpatch):

        # Define the replacement for the patched method:
        prefix = TESTVALUES['prefix']
        response_json = {
            "responseHeader":{},
            "response":{},
            "facet_counts": {
                "facet_fields":{
                    "version": [
                        '201611',1,
                        '201622',1,
                        '201633',1,
                    ]
                }
            }
        }
        mock_response = tests.mocks.responsemock.MockSolrResponse(success=True, content=json.dumps(response_json))
        getpatch.return_value = mock_response

        # Test variables
        testsolr = self.make_testsolr_with_access()
        drs_id = TESTVALUES['drs_id1']

        # Run code to be tested:
        resp = testsolr.retrieve_datasethandles_or_versionnumbers_of_allversions(drs_id)

        # Check result:
        expected = {'version_numbers': ['201622', '201611', '201633'], 'dataset_handles': None}

        ok = (set(resp) == set(expected))
        self.assertTrue(ok, 'Solr returned:\n'+str(resp)+'\nExpected:\n'+str(expected))


    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_retrieve_datasethandles_or_versionnumbers_of_allversions_both_patched(self, getpatch):

        # Define the replacement for the patched method:
        prefix = TESTVALUES['prefix']
        response_json = {
            "responseHeader":{},
            "response":{},
            "facet_counts": {
                "facet_fields":{
                    "version": [
                        '201611',1,
                        '201622',1,
                        '201633',1,
                    ],
                    "pid": [
                        prefix+'/123',1,
                        prefix+'/234',1,
                        prefix+'/345',1
                    ]
                }
            }
        }
        mock_response = tests.mocks.responsemock.MockSolrResponse(success=True, content=json.dumps(response_json))
        getpatch.return_value = mock_response

        # Test variables
        testsolr = self.make_testsolr_with_access()
        drs_id = TESTVALUES['drs_id1']

        # Run code to be tested:
        resp = testsolr.retrieve_datasethandles_or_versionnumbers_of_allversions(drs_id)

        # Check result:
        expected = {
            'version_numbers': ['201622', '201611', '201633'],
            'dataset_handles': ['hdl:'+prefix+'/345', 'hdl:'+prefix+'/234', 'hdl:'+prefix+'/123']
        }

        ok = (set(resp) == set(expected))
        self.assertTrue(ok, 'Solr returned:\n'+str(resp)+'\nExpected:\n'+str(expected))

    @mock.patch('esgfpid.solr.serverconnector.requests.get')
    def test_retrieve_datasethandles_or_versionnumbers_of_allversions_none_patched(self, getpatch):

        # Define the replacement for the patched method:
        prefix = TESTVALUES['prefix']
        response_json = {
            "responseHeader":{},
            "response":{},
            "facet_counts": {
                "facet_fields":{
                }
            }
        }
        mock_response = tests.mocks.responsemock.MockSolrResponse(success=True, content=json.dumps(response_json))
        getpatch.return_value = mock_response

        # Test variables
        testsolr = self.make_testsolr_with_access()
        drs_id = TESTVALUES['drs_id1']

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrResponseError):
            resp = testsolr.retrieve_datasethandles_or_versionnumbers_of_allversions(drs_id)



    def test_retrieve_datasethandles_or_versionnumbers_of_allversions_no_access(self):

        # Test variables
        testsolr = self.make_testsolr_without_access()
        drs_id = TESTVALUES['drs_id1']

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.SolrSwitchedOff):
            resp = testsolr.retrieve_datasethandles_or_versionnumbers_of_allversions(drs_id)


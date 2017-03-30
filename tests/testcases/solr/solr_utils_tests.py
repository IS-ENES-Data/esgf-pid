import unittest
import mock
import logging
import json
import esgfpid.solr.tasks.utils
import esgfpid.exceptions
import tests.utils

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Load some data that is needed for testing
PATH_RES = tests.utils.get_super_neighbour_directory(__file__, 'resources')
print(PATH_RES)
SOLR_RESPONSE = json.load(open(PATH_RES+'/solr_response.json'))

class SolrUtilsTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def test_extract_handles_from_response_json_ok(self):
        
        # Test variables
        response_json = SOLR_RESPONSE
        prefix = '10876.test'
  
        # Run code to be tested:
        received_handles = esgfpid.solr.tasks.utils.extract_file_handles_from_response_json(response_json, prefix)

        # Check result:
        expected_handles = [
            'hdl:10876.test/fe8b7a91-1391-4e36-a594-77d63f367846',
            'hdl:10876.test/045c78f2-90f5-465d-af68-54a23d7607d4',
            'hdl:10876.test/080c4b28-98ff-40ac-8d8f-76bea0db014e',
            'hdl:10876.test/079460fe-2ab8-461b-8bec-15b2028290a1',
            'hdl:10876.test/08bc91b7-6397-460c-977d-e3b4d3d185c6',
            'hdl:10876.test/04ebf501-0d5c-4207-b261-2507331fe081',
            'hdl:10876.test/e4f9bac0-f8ff-453f-a634-9953932801f6', 
            'hdl:10876.test/05935211-c8cc-4bf2-8149-745f41a7d07c'
        ]
        equal = (received_handles == expected_handles)
        self.assertTrue(equal, 'Expected: %s\nReceived: %s' % (expected_handles, received_handles))

    def test_no_handles_found(self):

        # Test variables
        prefix = 'whatever'
        response_json = {
            "bla": "blub",
            "facet_counts": {
                "facet_queries": { },
                "facet_fields": {
                    "handle": [ ],
                    "pid": [ ],
                    "tracking_id": [ ]
                },
            "facet_dates": { },
            "facet_ranges": { },
            "facet_intervals": { },
            "facet_heatmaps": { }
            }
        }

        # Run code to be tested:
        received_handles = esgfpid.solr.tasks.utils.extract_file_handles_from_response_json(response_json, prefix)

        # Check result:
        expected_handles = [] # None were found!
        equal = (received_handles == expected_handles)
        self.assertTrue(equal, 'Expected: %s\nReceived: %s' % (expected_handles, received_handles))

    def test_handles_found_ok(self):

        # Test variables
        prefix = 'myprefix'
        response_json = {
            "bla": "blub",
            "facet_counts": {
                "facet_queries": { },
                "facet_fields": {
                    "handle": [ ],
                    "pid": [ ],
                    "tracking_id": [
                        "hdl:myprefix/387b00d1-fe4e-40e3-a654-2dffbcf5bec6",
                        1,
                        "hdl:myprefix/79352fe9-1fbe-48b7-9b44-d6b36312296a",
                        1,
                        "hdl:myprefix/b9667250-b15c-47cf-b79c-5ab9fd49e5e1",
                        1
                    ]
                },
            "facet_dates": { },
            "facet_ranges": { },
            "facet_intervals": { },
            "facet_heatmaps": { }
            }
        }

        # Run code to be tested:
        received_handles = esgfpid.solr.tasks.utils.extract_file_handles_from_response_json(response_json, prefix)

        # Check result:
        expected_handles = ["hdl:myprefix/387b00d1-fe4e-40e3-a654-2dffbcf5bec6",
                            "hdl:myprefix/79352fe9-1fbe-48b7-9b44-d6b36312296a",
                            "hdl:myprefix/b9667250-b15c-47cf-b79c-5ab9fd49e5e1"]
        equal = (set(received_handles) == set(expected_handles))
        self.assertEqual(len(received_handles), len(expected_handles), 'Not the same number of handles')
        self.assertTrue(equal, 'Expected: %s\nReceived: %s' % (expected_handles, received_handles))

    def test_handles_found_without_prefix(self):
        ''' Prefix (and hdl:) should be put in front of handle! '''

        # Test variables
        prefix = 'myprefix'
        response_json = {
            "bla": "blub",
            "facet_counts": {
                "facet_queries": { },
                "facet_fields": {
                    "handle": [ ],
                    "pid": [ ],
                    "tracking_id": [
                        "387b00d1-fe4e-40e3-a654-2dffbcf5bec6",
                        1,
                        "79352fe9-1fbe-48b7-9b44-d6b36312296a",
                        1,
                        "b9667250-b15c-47cf-b79c-5ab9fd49e5e1",
                        1
                    ]
                },
            "facet_dates": { },
            "facet_ranges": { },
            "facet_intervals": { },
            "facet_heatmaps": { }
            }
        }

        # Run code to be tested:
        received_handles = esgfpid.solr.tasks.utils.extract_file_handles_from_response_json(response_json, prefix)

        # Check result:
        expected_handles = ["hdl:myprefix/387b00d1-fe4e-40e3-a654-2dffbcf5bec6",
                            "hdl:myprefix/79352fe9-1fbe-48b7-9b44-d6b36312296a",
                            "hdl:myprefix/b9667250-b15c-47cf-b79c-5ab9fd49e5e1"]
        equal = (set(received_handles) == set(expected_handles))
        self.assertEqual(len(received_handles), len(expected_handles), 'Not the same number of handles')
        self.assertTrue(equal, 'Expected: %s\nReceived: %s' % (expected_handles, received_handles))

    def test_handles_found_without_hdl(self):
        ''' hdl: should be put in front of the handle names!'''

        # Test variables
        prefix = 'myprefix'
        response_json = {
            "bla": "blub",
            "facet_counts": {
                "facet_queries": { },
                "facet_fields": {
                    "handle": [ ],
                    "pid": [ ],
                    "tracking_id": [
                        "myprefix/387b00d1-fe4e-40e3-a654-2dffbcf5bec6",
                        1,
                        "myprefix/79352fe9-1fbe-48b7-9b44-d6b36312296a",
                        1,
                        "myprefix/b9667250-b15c-47cf-b79c-5ab9fd49e5e1",
                        1
                    ]
                },
            "facet_dates": { },
            "facet_ranges": { },
            "facet_intervals": { },
            "facet_heatmaps": { }
            }
        }

        # Run code to be tested:
        received_handles = esgfpid.solr.tasks.utils.extract_file_handles_from_response_json(response_json, prefix)

        # Check result:
        expected_handles = ["hdl:myprefix/387b00d1-fe4e-40e3-a654-2dffbcf5bec6",
                            "hdl:myprefix/79352fe9-1fbe-48b7-9b44-d6b36312296a",
                            "hdl:myprefix/b9667250-b15c-47cf-b79c-5ab9fd49e5e1"]
        equal = (set(received_handles) == set(expected_handles))
        self.assertEqual(len(received_handles), len(expected_handles), 'Not the same number of handles')
        self.assertTrue(equal, 'Expected: %s\nReceived: %s' % (expected_handles, received_handles))

    def test_no_field_tracking_id(self):

        # Test variables
        prefix = 'myprefix'
        response_json = {
            "bla": "blub",
            "facet_counts": {
                "facet_queries": { },
                "facet_fields": {
                    "handle": [ ],
                    "pid": [ ]
                },
            "facet_dates": { },
            "facet_ranges": { },
            "facet_intervals": { },
            "facet_heatmaps": { }
            }
        }

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.SolrResponseError):
            received_handles = esgfpid.solr.tasks.utils.extract_file_handles_from_response_json(response_json, prefix)


    def test_no_facet_fields(self):

        # Test variables
        prefix = 'myprefix'
        response_json = {
            "bla": "blub",
            "facet_counts": {
                "facet_queries": { }
                },
            "facet_dates": { },
            "facet_ranges": { },
            "facet_intervals": { },
            "facet_heatmaps": { }
        }

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.SolrResponseError):
            received_handles = esgfpid.solr.tasks.utils.extract_file_handles_from_response_json(response_json, prefix)

    def test_response_is_none(self):

        # Test variables
        prefix = 'myprefix'
        response_json = None

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.SolrResponseError):
            received_handles = esgfpid.solr.tasks.utils.extract_file_handles_from_response_json(response_json, prefix)

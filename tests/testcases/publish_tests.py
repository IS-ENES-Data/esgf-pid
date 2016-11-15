import unittest
import mock
import logging
import json
import sys
import esgfpid.assistant.publish
import tests.mocks.responsemock
import tests.mocks.solrmock
import tests.mocks.rabbitmock
import tests.utils as utils
from tests.utils import compare_json_return_errormessage as error_message
from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS


# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import TESTVALUES as TESTVALUES


class PublishTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def __get_normal_init_args_dataset(self, testcoupler):
        return dict(
            prefix=TESTVALUES['prefix'],
            drs_id=TESTVALUES['drs_id1'],
            version_number=TESTVALUES['version_number1'],
            is_replica=False,
            thredds_service_path=TESTVALUES['thredds_service_path'],
            data_node=TESTVALUES['data_node'],
            coupler=testcoupler,
            consumer_solr_url='does_not_matter'
        )

    def __get_normal_rabbit_task_dataset(self):
        filehandle = 'hdl:'+TESTVALUES['prefix']+'/'+TESTVALUES['file_suffix1']
        return {
            "handle": "hdl:"+TESTVALUES['prefix']+'/'+TESTVALUES['suffix1'],
            "aggregation_level": "dataset",
            "operation": "publish",
            "message_timestamp":"anydate",
            "drs_id":TESTVALUES['drs_id1'],
            "version_number":TESTVALUES['version_number1'],
            "files":[filehandle],
            "is_replica":False,
            "data_node":TESTVALUES['data_node'],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'publication.dataset.orig',
            "consumer_solr_url": "does_not_matter"
        }

    def __get_normal_file_args(self):
        return dict(
            file_name = TESTVALUES['file_name1'],
            file_handle = 'hdl:'+TESTVALUES['prefix']+'/'+TESTVALUES['file_suffix1'],
            checksum = TESTVALUES['checksum1'],
            file_size = TESTVALUES['file_size1'],
            publish_path = TESTVALUES['publish_path1'],
            prefix = TESTVALUES['prefix'],
            checksum_type=TESTVALUES['checksum_type1'],
            file_version=TESTVALUES['file_version1'] # string
        )

    def __get_normal_rabbit_task_file(self):
        expected_data_url = ('http://'+TESTVALUES['data_node']+
            '/'+TESTVALUES['thredds_service_path']+
            '/'+TESTVALUES['publish_path1'])
        expected_rabbit_task = {
            "handle": "hdl:"+TESTVALUES['prefix']+'/'+TESTVALUES['file_suffix1'],
            "aggregation_level": "file",
            "operation": "publish",
            "message_timestamp":"anydate",
            "is_replica":False,
            "file_name": TESTVALUES['file_name1'],
            "data_url":expected_data_url,
            "data_node":TESTVALUES['data_node'],
            "file_size":TESTVALUES['file_size1'],
            "checksum":TESTVALUES['checksum1'],
            "checksum_type":TESTVALUES['checksum_type1'],
            "file_version":TESTVALUES['file_version1'],
            "parent_dataset":"hdl:"+TESTVALUES['prefix']+'/'+TESTVALUES['suffix1'],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'publication.file.orig',
        }
        return expected_rabbit_task

    def __make_patched_testcoupler(self, solr_off=False):
        testcoupler = esgfpid.coupling.Coupler(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_urls_open = 'rabbit_should_not_be_used',
            messaging_service_url_trusted = TESTVALUES['url_rabbit_trusted'],
            messaging_service_exchange_name = TESTVALUES['rabbit_exchange_name'],
            messaging_service_username_open = TESTVALUES['rabbit_username_open'],
            messaging_service_username_trusted = TESTVALUES['rabbit_username_trusted'],
            data_node = TESTVALUES['data_node'],
            thredds_service_path = TESTVALUES['thredds_service_path'],
            solr_url = 'solr_should_not_be_used',
            solr_https_verify=True,
            solr_switched_off=solr_off
        )
        # Replace objects that interact with servers with mocks
        # Patch rabbit
        rabbitmock = tests.mocks.rabbitmock.SimpleMockRabbitSender()
        testcoupler._Coupler__rabbit_message_sender = rabbitmock
        # Solr has to be atched for each test case!
        return testcoupler

    def __patch_coupler_with_solr_mock(self, testcoupler, previous_files, raise_error=None):
        if raise_error is None:
            solrmock = tests.mocks.solrmock.MockSolrInteractor(previous_files=previous_files)
        else:
            solrmock = tests.mocks.solrmock.MockSolrInteractor(previous_files=previous_files, raise_error=raise_error)
        testcoupler._Coupler__solr_sender = solrmock

    def __get_received_message_from_rabbit_mock(self, coupler, index):
        return coupler._Coupler__rabbit_message_sender.received_messages[index]

    def __make_test_assistant(self, testcoupler=None):
        if testcoupler is None:
            testcoupler = self.__make_patched_testcoupler()
        args = self.__get_normal_init_args_dataset(testcoupler)
        assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(**args)
        return assistant

    ### Actual test cases: ###

    #
    # Testing only init (passing info about dataset, but not about file yet):
    #

    def test_version_number_is_string_ok(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        args = self.__get_normal_init_args_dataset(testcoupler)
        args['is_replica'] = False
        args['version_number'] = str(args['version_number'])

        # Run code to be tested:
        assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(**args)

        # Check result:
        self.assertIsInstance(assistant, esgfpid.assistant.publish.DatasetPublicationAssistant,
            'Constructor failed.')

    def test_version_number_has_characters_error(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        args = self.__get_normal_init_args_dataset(testcoupler)
        args['is_replica'] = False
        args['version_number'] = 'abcdef'

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(**args)

    def test_init_normal(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        args = self.__get_normal_init_args_dataset(testcoupler)
        args['is_replica'] = False

        # Run code to be tested:
        assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(**args)

        # Check result:
        self.assertIsInstance(assistant, esgfpid.assistant.publish.DatasetPublicationAssistant,
            'Constructor failed.')

    def test_init_no_consumer_solr_url(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        args = self.__get_normal_init_args_dataset(testcoupler)
        args['is_replica'] = False
        del args['consumer_solr_url']

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(**args)

    def test_init_consumer_solr_url_is_none(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        args = self.__get_normal_init_args_dataset(testcoupler)
        args['is_replica'] = False
        args['consumer_solr_url'] = None

        # Run code to be tested:
        assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(**args)

        # Check result:
        self.assertIsInstance(assistant, esgfpid.assistant.publish.DatasetPublicationAssistant,
            'Constructor failed.')

    def test_init_string_replica_flag_ok(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        args = self.__get_normal_init_args_dataset(testcoupler)
        args['is_replica'] = 'False'

        # Run code to be tested:
        assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(**args)

        # Check result:
        self.assertIsInstance(assistant, esgfpid.assistant.publish.DatasetPublicationAssistant,
            'Constructor failed.')
        self.assertFalse(assistant._DatasetPublicationAssistant__is_replica, 'Replica flag should be False if it is passed as "False".')
    
    def test_init_string_wrong_replica_flag(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        args = self.__get_normal_init_args_dataset(testcoupler)
        args['is_replica'] = 'Maybe'

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.ArgumentError) as raised:
            assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(**args)
        self.assertIn('"Maybe" could not be parsed to boolean', raised.exception.message,
            'Unexpected error message: %s' % raised.exception.message)

    #
    # Testing entire publication
    # First publication, no previous info on solr, no consistency check
    #

    def test_normal_publication_ok(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        assistant = self.__make_test_assistant(testcoupler)
        args = self.__get_normal_file_args()

        # Run code to be tested:
        assistant.add_file(**args)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        expected_rabbit_task = self.__get_normal_rabbit_task_dataset()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 1)
        expected_rabbit_task = self.__get_normal_rabbit_task_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_normal_publication_sev_files_ok(self):

        # Test variables
        prefix = TESTVALUES['prefix']
        handle1 = 'hdl:'+prefix+'/456'
        handle2 = 'hdl:'+prefix+'/789'

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        assistant = self.__make_test_assistant(testcoupler)
        args1 = self.__get_normal_file_args()
        args1['file_handle'] = handle1
        args2 = self.__get_normal_file_args()
        args2['file_handle'] = handle2

        # Run code to be tested:
        assistant.add_file(**args1)
        assistant.add_file(**args2)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        expected_rabbit_task = self.__get_normal_rabbit_task_dataset()
        expected_rabbit_task['files'] = [handle2, handle1]
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 1)
        expected_rabbit_task = self.__get_normal_rabbit_task_file()
        expected_rabbit_task['handle'] = handle1
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 2)
        expected_rabbit_task = self.__get_normal_rabbit_task_file()
        expected_rabbit_task['handle'] = handle2
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_file_wrong_prefix(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        assistant = self.__make_test_assistant(testcoupler)
        args = self.__get_normal_file_args()
        args['file_handle'] = 'hdl:1234/56789'

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.ESGFException) as raised:
            assistant.add_file(**args)
        self.assertIn('ESGF rule violation', raised.exception.message,
            'Unexpected message: %s' % raised.exception.message)

    def test_add_file_without_hdl_in_handle(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        assistant = self.__make_test_assistant(testcoupler)
        args = self.__get_normal_file_args()
        handle = TESTVALUES['prefix']+'/'+TESTVALUES['file_suffix1']
        args['file_handle'] = handle

        # Run code to be tested:
        assistant.add_file(**args)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        expected_rabbit_task = self.__get_normal_rabbit_task_dataset()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 1)
        expected_rabbit_task = self.__get_normal_rabbit_task_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_file_fileversion_as_integer_filesize_as_string(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        assistant = self.__make_test_assistant(testcoupler)
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        args = self.__get_normal_file_args()
        args['file_version'] = int(args['file_version'])
        args['file_size'] = str(args['file_size'])

        # Run code to be tested:
        assistant.add_file(**args)
        assistant.dataset_publication_finished()

        # Check result (file):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 1)
        expected_rabbit_task = self.__get_normal_rabbit_task_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_file_filesize_has_characters(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        assistant = self.__make_test_assistant(testcoupler)
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        args = self.__get_normal_file_args()
        args['file_size'] = 'abc'

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant.add_file(**args)
        #assistant.dataset_publication_finished()

        # Check result (file):
        #received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 1)
        #expected_rabbit_task = self.__get_normal_rabbit_task_file()
        #same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        #self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_normal_publication_without_solr_access(self):
        '''
        Solr was switched off and raises an errors, but this is caught.
        The consistency check is not run, but the rest of the publication is the same.
        '''

        # Preparations:  
        testcoupler = self.__make_patched_testcoupler()
        # Patch the solr module with a different mock:
        error_to_be_raised = esgfpid.exceptions.SolrSwitchedOff
        self.__patch_coupler_with_solr_mock(testcoupler, None, error_to_be_raised) # makes mock raise this error
        assistant = self.__make_test_assistant(testcoupler)
        # Other variables:
        args = self.__get_normal_file_args()

        # Run code to be tested:
        assistant.add_file(**args)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        expected_rabbit_task = self.__get_normal_rabbit_task_dataset()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 1)
        expected_rabbit_task = self.__get_normal_rabbit_task_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_normal_publication_solr_returns_none(self):

        # Preparations:  
        testcoupler = self.__make_patched_testcoupler()
        # Patch the solr module with a different mock:
        self.__patch_coupler_with_solr_mock(testcoupler, 'NONE') # makes mock return None - this should never happen in reality!
        assistant = self.__make_test_assistant(testcoupler)
        # Other variables:
        args = self.__get_normal_file_args()

        # Run code to be tested:
        assistant.add_file(**args)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        expected_rabbit_task = self.__get_normal_rabbit_task_dataset()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    #
    # Testing entire RE-publication
    # Republication, solr provides info on previous files, consistency check
    #

    def test_normal_publication_with_pos_consis_check(self):

        # Test variables:
        args = self.__get_normal_file_args()
        prev = [args['file_handle']]
        # Preparations:  
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, prev) # solr returns file list
        assistant = self.__make_test_assistant(testcoupler)

        # Run code to be tested:
        assistant.add_file(**args)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        expected_rabbit_task = self.__get_normal_rabbit_task_dataset()
        expected_rabbit_task['files'] = [args['file_handle']]
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 1)
        expected_rabbit_task = self.__get_normal_rabbit_task_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_normal_publication_with_pos_consis_check_missing_hdl(self):

        # Test variables:
        args = self.__get_normal_file_args()
        handle_no_hdl = TESTVALUES['prefix']+'/'+TESTVALUES['file_suffix1']
        handle_with_hdl = 'hdl:'+handle_no_hdl
        prev = [handle_with_hdl] # solr always adds hdl before returning it!
        args['file_handle'] = handle_no_hdl # Handle without hdl is published!
        # Preparations:  
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, prev) # solr returns file list
        assistant = self.__make_test_assistant(testcoupler)

        # Run code to be tested:
        assistant.add_file(**args)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        expected_rabbit_task = self.__get_normal_rabbit_task_dataset()
        expected_rabbit_task['files'] = ['hdl:'+args['file_handle']]
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 1)
        expected_rabbit_task = self.__get_normal_rabbit_task_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_normal_publication_with_neg_consis_check_missing_file(self):

        # Test variables:
        file_handle = TESTVALUES['prefix']+'/'+TESTVALUES['file_suffix1']
        another_file_handle = TESTVALUES['prefix']+'/random_suffix_abc123'
        prev = [file_handle, another_file_handle]
        file_name = TESTVALUES['file_name1']
        checksum = TESTVALUES['checksum1']
        file_size = TESTVALUES['file_size1']
        publish_path = TESTVALUES['publish_path1']
        prefix = TESTVALUES['prefix']
        checksum_type=TESTVALUES['checksum_type1']

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, prev) # solr returns file list
        assistant = self.__make_test_assistant(testcoupler)

        # Prepare:
        assistant.add_file(
            file_name=file_name,
            file_handle=file_handle,
            checksum=checksum,
            file_size=file_size,
            publish_path=publish_path,
            checksum_type=checksum_type,
            file_version=1
        )

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.InconsistentFilesetException):
            assistant.dataset_publication_finished()

    def test_normal_publication_with_neg_consis_check_too_many_files(self):

        # Test variables:
        file_handle = TESTVALUES['prefix']+'/'+TESTVALUES['file_suffix1']
        prev = [file_handle]
        file_name = TESTVALUES['file_name1']
        checksum = TESTVALUES['checksum1']
        file_size = TESTVALUES['file_size1']
        publish_path = TESTVALUES['publish_path1']
        prefix = TESTVALUES['prefix']
        file_handle2 = TESTVALUES['prefix']+'/'+TESTVALUES['file_suffix2']
        checksum_type=TESTVALUES['checksum_type1']

        # Prepare:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, prev) # solr returns file list
        assistant = self.__make_test_assistant(testcoupler)

        assistant.add_file(
            file_name=file_name,
            file_handle=file_handle,
            checksum=checksum,
            file_size=file_size,
            publish_path=publish_path,
            checksum_type=checksum_type,
            file_version=1
        )
        assistant.add_file(
            file_name=file_name,
            file_handle=file_handle2,
            checksum=checksum,
            file_size=file_size,
            publish_path=publish_path,
            checksum_type=checksum_type,
            file_version=1
        )

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.InconsistentFilesetException):
            assistant.dataset_publication_finished()

    #
    # Testing invalid operations
    # (publication actions occur at the wrong time / wrong state)
    #

    def test_add_file_too_late(self):

        # Test variables:
        args = self.__get_normal_file_args()

        # Prepare:
        testcoupler = self.__make_patched_testcoupler(solr_off=True)
        assistant = self.__make_test_assistant(testcoupler)
        assistant.add_file(**args)
        assistant.dataset_publication_finished()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.OperationUnsupportedException):
            assistant.add_file(**args)

    def test_finish_too_late(self):

        # Test variables:
        args = self.__get_normal_file_args()

        # Preparations
        testcoupler = self.__make_patched_testcoupler(solr_off=True)
        assistant = self.__make_test_assistant(testcoupler)
        assistant.add_file(**args)
        assistant.dataset_publication_finished()

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.OperationUnsupportedException):
            assistant.dataset_publication_finished()

    def test_finish_too_early(self):

        # Test variables:
        testcoupler = self.__make_patched_testcoupler(solr_off=True)
        assistant = self.__make_test_assistant(testcoupler)

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.OperationUnsupportedException):
            assistant.dataset_publication_finished()

    #
    # Testing replica publication
    #

    def test_normal_publication_replica_flag_bool_ok(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler(solr_off=True) # solr switched off, no consistency check
        args = self.__get_normal_init_args_dataset(testcoupler)
        args['is_replica'] = True
        assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(**args)
        fileargs = self.__get_normal_file_args()

        # Run code to be tested:
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        expected_rabbit_task = self.__get_normal_rabbit_task_dataset()
        expected_rabbit_task['is_replica'] = True
        expected_rabbit_task['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.dataset.replica'
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 1)
        expected_rabbit_task = self.__get_normal_rabbit_task_file()
        expected_rabbit_task['is_replica'] = True
        expected_rabbit_task['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.file.replica'
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_normal_publication_replica_flag_string_ok(self):

        # Preparations:
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_coupler_with_solr_mock(testcoupler, None) # Solr returns []
        args = self.__get_normal_init_args_dataset(testcoupler)
        args['is_replica'] = 'True'
        assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(**args)
        fileargs = self.__get_normal_file_args()

        # Run code to be tested:
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 0)
        expected_rabbit_task = self.__get_normal_rabbit_task_dataset()
        expected_rabbit_task['is_replica'] = True
        expected_rabbit_task['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.dataset.replica'
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = self.__get_received_message_from_rabbit_mock(testcoupler, 1)
        expected_rabbit_task = self.__get_normal_rabbit_task_file()
        expected_rabbit_task['is_replica'] = True
        expected_rabbit_task['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.file.replica'
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    #
    # Helper tests
    #

    def test_get_dataset_handle(self):

        # Preparations:
        assistant = self.__make_test_assistant()

        # Run code to be tested:
        handle = assistant.get_dataset_handle()

        # Check result:
        exp_handle = 'hdl:'+TESTVALUES['prefix']+'/'+TESTVALUES['suffix1']
        self.assertEqual(handle, exp_handle,
            'Wrong handle returned.')
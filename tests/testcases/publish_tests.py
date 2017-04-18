import unittest
import logging
import tests.utils as utils
from tests.utils import compare_json_return_errormessage as error_message

from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS
from esgfpid.assistant.publish import DatasetPublicationAssistant
from esgfpid.exceptions import SolrSwitchedOff

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS


class PublishTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    #
    # Testing only init (passing info about dataset, but not about file yet):
    #

    def test_normal_ok(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_empty_file_list(testcoupler)
        args = TESTHELPERS.get_args_for_publication_assistant()

        # Run code to be tested:
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **args)

        # Check result:
        self.assertIsInstance(assistant, DatasetPublicationAssistant,
            'Constructor failed.')

    def test_version_number_is_string_ok(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        args = TESTHELPERS.get_args_for_publication_assistant()
        args['version_number'] = str(args['version_number'])

        # Run code to be tested:
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **args)

        # Check result:
        self.assertIsInstance(assistant, DatasetPublicationAssistant,
            'Constructor failed.')

    def test_version_number_has_characters_error(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        args = TESTHELPERS.get_args_for_publication_assistant()
        args['version_number'] = 'abcdef'

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant = DatasetPublicationAssistant(coupler=testcoupler, **args)

    def test_init_no_consumer_solr_url(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        args = TESTHELPERS.get_args_for_publication_assistant()
        del args['consumer_solr_url']

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant = DatasetPublicationAssistant(**args)

    def test_init_consumer_solr_url_is_none(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        args = TESTHELPERS.get_args_for_publication_assistant()
        args['consumer_solr_url'] = None

        # Run code to be tested:
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **args)

        # Check result:
        self.assertIsInstance(assistant, DatasetPublicationAssistant,
            'Constructor failed.')

    def test_init_string_replica_flag_ok(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        args = TESTHELPERS.get_args_for_publication_assistant()
        args['is_replica'] = 'False'

        # Run code to be tested:
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **args)

        # Check result:
        self.assertIsInstance(assistant, DatasetPublicationAssistant,
            'Constructor failed.')
        self.assertFalse(assistant._DatasetPublicationAssistant__is_replica,
            'Replica flag should be False if it is passed as "False".')
    
    def test_init_string_wrong_replica_flag(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        args = TESTHELPERS.get_args_for_publication_assistant()
        args['is_replica'] = 'Maybe'

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.ArgumentError) as raised:
            assistant = DatasetPublicationAssistant(coupler=testcoupler, **args)
        self.assertIn('"Maybe" could not be parsed to boolean', raised.exception.message,
            'Unexpected error message: %s' % raised.exception.message)

    #
    # Testing entire publication
    # First publication, no previous info on solr, no consistency check
    #

    def test_normal_publication_ok(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        fileargs = TESTHELPERS.get_args_for_adding_file()

        # Run code to be tested:
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 0)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_dataset()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 1)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_normal_publication_sev_files_ok(self):

        # Test variables
        handle1 = PREFIX_WITH_HDL+'/456'
        handle2 = PREFIX_WITH_HDL+'/789'

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        args1 = TESTHELPERS.get_args_for_adding_file()
        args1['file_handle'] = handle1
        args2 = TESTHELPERS.get_args_for_adding_file()
        args2['file_handle'] = handle2

        # Run code to be tested:
        assistant.add_file(**args1)
        assistant.add_file(**args2)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 0)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_dataset()
        expected_rabbit_task['files'] = [handle2, handle1]
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 1)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_file()
        expected_rabbit_task['handle'] = handle1
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 2)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_file()
        expected_rabbit_task['handle'] = handle2
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_file_wrong_prefix(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        fileargs = TESTHELPERS.get_args_for_adding_file()
        fileargs['file_handle'] = 'hdl:1234/56789'

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.ESGFException) as raised:
            assistant.add_file(**fileargs)
        self.assertIn('ESGF rule violation', raised.exception.message,
            'Unexpected message: %s' % raised.exception.message)

    def test_add_file_without_hdl_in_handle(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        fileargs = TESTHELPERS.get_args_for_adding_file()
        fileargs['file_handle'] = FILEHANDLE_NO_HDL

         # Run code to be tested:
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 0)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_dataset()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 1)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_file_fileversion_as_integer_filesize_as_string(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        fileargs = TESTHELPERS.get_args_for_adding_file()
        fileargs['file_version'] = int(fileargs['file_version'])
        fileargs['file_size'] = str(fileargs['file_size'])

        # Run code to be tested:
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Check result (file):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 1)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_add_file_filesize_has_characters(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        fileargs = TESTHELPERS.get_args_for_adding_file()
        fileargs['file_size'] = 'abc'

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            assistant.add_file(**fileargs)

    def test_normal_publication_without_solr_access(self):
        '''
        Solr was switched off and raises an errors, but this is caught.
        The consistency check is not run, but the rest of the publication is the same.
        '''

        # Preparations:  
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        TESTHELPERS.patch_solr_raises_error(testcoupler, SolrSwitchedOff) # solr raises error
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        fileargs = TESTHELPERS.get_args_for_adding_file()

        # Run code to be tested:
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 0)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_dataset()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 1)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_normal_publication_solr_returns_none(self):

        # Preparations:  
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, None) # makes mock return None - this should never happen in reality!
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        fileargs = TESTHELPERS.get_args_for_adding_file()

        # Run code to be tested:
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 0)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_dataset()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    #
    # Testing entire RE-publication
    # Republication, solr provides info on previous files, consistency check
    #

    def test_normal_publication_with_pos_consis_check(self):

        # Test variables:
        fileargs = TESTHELPERS.get_args_for_adding_file()
        prev_list = [FILEHANDLE_HDL]

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev_list) # solr returns file list
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)

        # Run code to be tested:
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 0)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_dataset()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 1)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_normal_publication_with_pos_consis_check_missing_hdl(self):

        # Test variables:
        # File to be published (without hdl):
        fileargs = TESTHELPERS.get_args_for_adding_file()
        fileargs['file_handle'] = FILEHANDLE_NO_HDL
        # Preparations:  
        prev_list = [FILEHANDLE_HDL] # solr always adds hdl before returning it!
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev_list) # solr returns file list
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)

        # Run code to be tested:
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 0)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_dataset()
        expected_rabbit_task['files'] = [FILEHANDLE_HDL]
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 1)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_file()
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_normal_publication_with_neg_consis_check_missing_file(self):

        # Test variables:
        args1 = TESTHELPERS.get_args_for_adding_file()
        prev1 = args1['file_handle']
        prev2 = PREFIX_NO_HDL +'/random_suffix_abc123'
        prev_list = [prev1, prev2]

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev_list) # solr returns file list
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)

        # Prepare:
        assistant.add_file(**args1)

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.InconsistentFilesetException):
            assistant.dataset_publication_finished()

    def test_normal_publication_with_neg_consis_check_too_many_files(self):

        # Test variables:
        args1 = TESTHELPERS.get_args_for_adding_file()
        args2 = TESTHELPERS.get_args_for_adding_file()
        handle1 = args1['file_handle']
        handle2 = PREFIX_NO_HDL +'/random_suffix_abc123'
        args2['file_handle'] = handle2
        prev_list = [handle1]

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev_list) # solr returns file list
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)

        # Prepare:
        assistant.add_file(**args1)
        assistant.add_file(**args2)

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.InconsistentFilesetException):
            assistant.dataset_publication_finished()

    #
    # Testing invalid operations
    # (publication actions occur at the wrong time / wrong state)
    #

    def test_add_file_too_late(self):

        # Prepare:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True) # solr switched off, no consistency check
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        fileargs = TESTHELPERS.get_args_for_adding_file()
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.OperationUnsupportedException):
            assistant.add_file(**fileargs)

    def test_finish_too_late(self):

        # Prepare:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True) # solr switched off, no consistency check
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        fileargs = TESTHELPERS.get_args_for_adding_file()
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Run code to be tested:
        with self.assertRaises(esgfpid.exceptions.OperationUnsupportedException):
            assistant.dataset_publication_finished()

    def test_finish_too_early(self):

        # Test variables:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True) # solr switched off, no consistency check
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.OperationUnsupportedException):
            assistant.dataset_publication_finished()

    #
    # Testing replica publication
    #

    def test_normal_publication_replica_flag_bool_ok(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True) # solr switched off, no consistency check
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        dsargs['is_replica'] = True
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        fileargs = TESTHELPERS.get_args_for_adding_file()

        # Run code to be tested:
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 0)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_dataset()
        expected_rabbit_task['is_replica'] = True
        expected_rabbit_task['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.dataset.replica'
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 1)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_file()
        expected_rabbit_task['is_replica'] = True
        expected_rabbit_task['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.file.replica'
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    def test_normal_publication_replica_flag_string_ok(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True) # solr switched off, no consistency check
        TESTHELPERS.patch_with_rabbit_mock(testcoupler)
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        dsargs['is_replica'] = True
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)
        fileargs = TESTHELPERS.get_args_for_adding_file()

        # Run code to be tested:
        assistant.add_file(**fileargs)
        assistant.dataset_publication_finished()

        # Check result (dataset):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 0)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_dataset()
        expected_rabbit_task['is_replica'] = True
        expected_rabbit_task['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.dataset.replica'
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

        # Check result (file):
        received_rabbit_task = TESTHELPERS.get_received_message_from_rabbitmock(testcoupler, 1)
        expected_rabbit_task = TESTHELPERS.get_rabbit_message_publication_file()
        expected_rabbit_task['is_replica'] = True
        expected_rabbit_task['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.file.replica'
        same = utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(same, error_message(expected_rabbit_task, received_rabbit_task))

    #
    # Helper tests
    #

    def test_get_dataset_handle(self):

        # Preparations:
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True) # solr switched off, no consistency check
        dsargs = TESTHELPERS.get_args_for_publication_assistant()
        dsargs['is_replica'] = True
        assistant = DatasetPublicationAssistant(coupler=testcoupler, **dsargs)

        # Run code to be tested:
        handle = assistant.get_dataset_handle()

        # Check result:
        exp_handle = DATASETHANDLE_HDL
        self.assertEqual(handle, exp_handle,
            'Wrong handle returned.')
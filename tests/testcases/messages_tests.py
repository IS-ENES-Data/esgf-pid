import unittest
import logging
import copy
import tests.utils as utils
from tests.utils import compare_json_return_errormessage as error_message

from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS
import esgfpid.exceptions
import esgfpid.assistant.messages as messages

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class MessageCreationTestcase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def __get_args_dict_file(self):
        return dict(
            file_handle='123/456',
            is_replica='mightbe',
            file_size='333bytes',
            file_name='filey.nc',
            checksum='xyz',
            data_url='myurl.de',
            data_node='dkrz.de',
            parent_dataset='abc/def',
            timestamp='todayish',
            checksum_type='SHA999',
            file_version='v7'
        )

    def __get_args_dict_dataset(self):
        return dict(
            dataset_handle='abc/def',
            is_replica='mightbe',
            drs_id='drs/id/foo',
            version_number='201699',
            list_of_files='listof_files',
            data_node='dkrz.de',
            timestamp='todayish'
        )

    ### Actual test cases: ###

    def test_make_message_publication_file_ok(self):

        # Test variables
        args_dict = self.__get_args_dict_file()
  
        # Run code to be tested:
        received_message = messages.publish_file(**args_dict)

        # Check result:
        expected = copy.deepcopy(args_dict)
        expected['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.file.orig'
        expected['aggregation_level'] = 'file'
        expected['operation'] = 'publish'
        # Rename some:
        expected['handle'] = expected['file_handle']
        del expected['file_handle']
        expected['message_timestamp'] = expected['timestamp']
        del expected['timestamp']

        same = utils.is_json_same(expected, received_message)
        self.assertTrue(same, error_message(expected, received_message))

    def test_make_message_publication_file_replica_ok(self):

        # Test variables
        args_dict = self.__get_args_dict_file()
        args_dict['is_replica'] = True
  
        # Run code to be tested:
        received_message = messages.publish_file(**args_dict)

        # Check result:
        expected = copy.deepcopy(args_dict)
        expected['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.file.replica'
        expected['aggregation_level'] = 'file'
        expected['operation'] = 'publish'
        # Rename some:
        expected['handle'] = expected['file_handle']
        del expected['file_handle']
        expected['message_timestamp'] = expected['timestamp']
        del expected['timestamp']

        same = utils.is_json_same(expected, received_message)
        self.assertTrue(same, error_message(expected, received_message))

    def test_make_message_publication_dataset_ok(self):

        # Test variables
        args_dict = self.__get_args_dict_dataset()
  
        # Run code to be tested:
        received_message = messages.publish_dataset(**args_dict)

        # Check result:
        expected = copy.deepcopy(args_dict)
        expected['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.dataset.orig'
        expected['aggregation_level'] = 'dataset'
        expected['operation'] = 'publish'
        # Rename some:
        expected['handle'] = expected['dataset_handle']
        del expected['dataset_handle']
        expected['message_timestamp'] = expected['timestamp']
        del expected['timestamp']
        expected['files'] = expected['list_of_files']
        del expected['list_of_files']

        same = utils.is_json_same(expected, received_message)
        self.assertTrue(same, error_message(expected, received_message))


    def test_make_message_publication_dataset_replica_ok(self):

        # Test variables
        args_dict = self.__get_args_dict_dataset()
        args_dict['is_replica'] = True
  
        # Run code to be tested:
        received_message = messages.publish_dataset(**args_dict)

        # Check result:
        expected = copy.deepcopy(args_dict)
        expected['ROUTING_KEY'] = ROUTING_KEY_BASIS+'publication.dataset.replica'
        expected['aggregation_level'] = 'dataset'
        expected['operation'] = 'publish'
        # Rename some:
        expected['handle'] = expected['dataset_handle']
        del expected['dataset_handle']
        expected['message_timestamp'] = expected['timestamp']
        del expected['timestamp']
        expected['files'] = expected['list_of_files']
        del expected['list_of_files']

        same = utils.is_json_same(expected, received_message)
        self.assertTrue(same, error_message(expected, received_message))


    def test_make_message_publication_dataset_missing_args(self):

        # Test variables
        args_dict = self.__get_args_dict_dataset()
  
        # Run code to be tested:
        for k,v in args_dict.iteritems():
            args = copy.deepcopy(args_dict)
            LOGGER.debug('Deleting %s and trying...', k)
            del args[k]
            with self.assertRaises(esgfpid.exceptions.ArgumentError):
                received_message = messages.publish_dataset(**args)


    def test_make_message_unpublish_allversions_consumer_must_find_versions_ok(self):

        # Test variables
        args_dict = dict(
            drs_id = 'abc',
            data_node = 'dkrz.de',
            timestamp = 'todayish'
        )

        # Run code to be tested:
        received_message = messages.unpublish_allversions_consumer_must_find_versions(**args_dict)

        # Check result:
        expected = copy.deepcopy(args_dict)
        expected['ROUTING_KEY'] = ROUTING_KEY_BASIS+'unpublication.all'
        expected['aggregation_level'] = 'dataset'
        expected['operation'] = 'unpublish_all_versions'
        # Rename some:
        expected['message_timestamp'] =  expected['timestamp']
        del expected['timestamp']

        same = utils.is_json_same(expected, received_message)
        self.assertTrue(same, error_message(expected, received_message))


    def test_make_message_unpublish_one_version_ok(self):

        # Test variables
        args_dict = dict(
            dataset_handle = 'abc/def',
            data_node = 'dkrz.de',
            timestamp = 'todayish',
            drs_id = 'mytest'
        )

        # Run code to be tested:
        received_message = messages.unpublish_one_version(**args_dict)

        # Check result:
        expected = copy.deepcopy(args_dict)
        expected['ROUTING_KEY'] = ROUTING_KEY_BASIS+'unpublication.one'
        expected['aggregation_level'] = 'dataset'
        expected['operation'] = 'unpublish_one_version'
        # Rename some:
        expected['handle'] = expected['dataset_handle']
        del expected['dataset_handle']
        expected['message_timestamp'] = expected['timestamp']
        del expected['timestamp']

        same = utils.is_json_same(expected, received_message)
        self.assertTrue(same, error_message(expected, received_message))


    def test_make_message_add_errata_ids_ok(self):

        # Test variables
        args_dict = dict(
            dataset_handle = 'abc/def',
            errata_ids = 'dkrz.de',
            timestamp = 'todayish',
            drs_id = 'drs_foo',
            version_number = 'vers_foo'
        )

        # Run code to be tested:
        received_message = messages.add_errata_ids_message(**args_dict)

        # Check result:
        expected = copy.deepcopy(args_dict)
        expected['ROUTING_KEY'] = ROUTING_KEY_BASIS+'errata.add'
        expected['operation'] = 'add_errata_ids'
        expected['drs_id'] = 'drs_foo'
        expected['version_number'] = 'vers_foo'
        # Rename some:
        expected['handle'] = expected['dataset_handle']
        del expected['dataset_handle']
        expected['message_timestamp'] = expected['timestamp']
        del expected['timestamp']

        same = utils.is_json_same(expected, received_message)
        self.assertTrue(same, error_message(expected, received_message))

    def test_make_message_remove_errata_ids_ok(self):

        # Test variables
        args_dict = dict(
            dataset_handle = 'abc/def',
            errata_ids = 'dkrz.de',
            timestamp = 'todayish',
            drs_id = 'drs_foo',
            version_number = 'vers_foo'
        )

        # Run code to be tested:
        received_message = messages.remove_errata_ids_message(**args_dict)

        # Check result:
        expected = copy.deepcopy(args_dict)
        expected['ROUTING_KEY'] = ROUTING_KEY_BASIS+'errata.remove'
        expected['operation'] = 'remove_errata_ids'
        expected['drs_id'] = 'drs_foo'
        expected['version_number'] = 'vers_foo'
        # Rename some:
        expected['handle'] = expected['dataset_handle']
        del expected['dataset_handle']
        expected['message_timestamp'] = expected['timestamp']
        del expected['timestamp']

        same = utils.is_json_same(expected, received_message)
        self.assertTrue(same, error_message(expected, received_message))
import unittest
import logging
import esgfpid

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class HandleStringTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    ### Actual test cases: ###

    def test_handle_string_data_cart(self):
        '''
        Making sure that the handle string created for a 
        data cart containing just one dataset is not the
        same as for the dataset itself.
        '''

        # Test variables
        drs_id = 'foobar'
        vers_num = 20160101
        prefix = 'myprefix'

        # Run code
        dataset_id = esgfpid.utils.concatenate_drs_and_versionnumber(
            drs_id,
            vers_num
        )
        dataset_pid = esgfpid.assistant.publish.create_dataset_handle(
            drs_id = drs_id,
            version_number = vers_num,
            prefix = prefix
        )
        dict_of_drs_ids_and_pids = {
            dataset_id:dataset_pid
        }
        datacart_pid = esgfpid.assistant.datacart.get_handle_string_for_datacart(
            dict_of_drs_ids_and_pids,
            prefix
        )

        # Check results
        self.assertNotEquals(dataset_pid, datacart_pid)
import unittest
import mock
import logging
import json
import sys
sys.path.append("..")
import tests.mocks.solrmock
import tests.mocks.rabbitmock
import esgfpid

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import TESTVALUES as TESTVALUES


class ConsistencyTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')

    def tearDown(self):
        LOGGER.info('#############################')

    def __get_args_dict(self, testcoupler):
        return dict(
            drs_id = TESTVALUES['drs_id1'],
            data_node = TESTVALUES['data_node'],
            version_number = TESTVALUES['version_number1'],
            coupler = testcoupler
        )

    def __make_patched_testcoupler(self, solr_off=False):
        testcoupler = esgfpid.coupling.Coupler(
            handle_prefix = TESTVALUES['prefix'],
            messaging_service_urls = 'rabbit_should_not_be_used',
            messaging_service_url_preferred = None,
            messaging_service_exchange_name = TESTVALUES['messaging_exchange'],
            messaging_service_username = TESTVALUES['rabbit_username'],
            messaging_service_password = TESTVALUES['rabbit_password'],
            data_node = TESTVALUES['data_node'],
            thredds_service_path = TESTVALUES['thredds_service_path'],
            solr_url = 'solr_should_not_be_used',
            solr_https_verify=True,
            solr_switched_off=solr_off
        )
        return testcoupler

    def __patch_testcoupler_with_solr_mock(self, testcoupler, previous_files, raise_error=None):
        if raise_error is None:
            solrmock = tests.mocks.solrmock.MockSolrInteractor(previous_files=previous_files)
        else:
            solrmock = tests.mocks.solrmock.MockSolrInteractor(previous_files=previous_files, raise_error=raise_error)
        testcoupler._Coupler__solr_sender = solrmock

    def test_preparation_ok(self):

        # Prepare
        testcoupler = self.__make_patched_testcoupler()
        prev = ['abc', 'def']
        self.__patch_testcoupler_with_solr_mock(testcoupler, prev)
        args = self.__get_args_dict(testcoupler)

        # Run code to be tested:
        checker = esgfpid.assistant.consistency.Checker(**args)

        # Check result:
        self.assertIsInstance(checker, esgfpid.assistant.consistency.Checker,
            'Preparation failed.')
        self.assertTrue(checker.can_run_check(),
            'With previous files, check should be enabled, but isn\'t.')

    def test_check_ok(self):

        # Prepare
        testcoupler = self.__make_patched_testcoupler()
        prev = ['abc', 'def']
        self.__patch_testcoupler_with_solr_mock(testcoupler, prev)
        args = self.__get_args_dict(testcoupler)
        checker = esgfpid.assistant.consistency.Checker(**args)

        # Run code to be tested:
        success = checker.data_consistency_check(prev)

        # Check result:
        self.assertTrue(success, 'The check should pass.')

    def test_check_too_many_files(self):

        # Prepare
        testcoupler = self.__make_patched_testcoupler()
        prev = ['abc', 'def']
        too_many_files = prev+['ghi', 'jkl']
        self.__patch_testcoupler_with_solr_mock(testcoupler, prev)
        args = self.__get_args_dict(testcoupler)
        checker = esgfpid.assistant.consistency.Checker(**args)

        # Run code to be tested:
        success = checker.data_consistency_check(too_many_files)

        # Check result:
        self.assertFalse(success, 'The check should fail - too many files passed.')

    def test_check_too_few_files(self):

        # Prepare
        testcoupler = self.__make_patched_testcoupler()
        prev = ['abc', 'def']
        too_few_files = ['abc']
        self.__patch_testcoupler_with_solr_mock(testcoupler, prev)
        args = self.__get_args_dict(testcoupler)
        checker = esgfpid.assistant.consistency.Checker(**args)

        # Run code to be tested:
        success = checker.data_consistency_check(too_few_files)

        # Check result:
        self.assertFalse(success, 'The check should fail - too few files passed.')

    def test_check_different_files(self):

        # Prepare
        testcoupler = self.__make_patched_testcoupler()
        prev = ['abc', 'def']
        different_files = ['xyz', 'zyx']
        self.__patch_testcoupler_with_solr_mock(testcoupler, prev)
        args = self.__get_args_dict(testcoupler)
        checker = esgfpid.assistant.consistency.Checker(**args)

        # Run code to be tested:
        success = checker.data_consistency_check(different_files)

        # Check result:
        self.assertFalse(success, 'The check should fail - different files passed.')

    def test_check_no_files(self):

        # Prepare
        testcoupler = self.__make_patched_testcoupler()
        prev = ['abc', 'def']
        no_files = []
        self.__patch_testcoupler_with_solr_mock(testcoupler, prev)
        args = self.__get_args_dict(testcoupler)
        checker = esgfpid.assistant.consistency.Checker(**args)

        # Run code to be tested:
        with self.assertRaises(ValueError):
            success = checker.data_consistency_check(no_files)

    def test_check_try_if_disabled(self):

        # Prepare
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_testcoupler_with_solr_mock(testcoupler, 'any_previous_files')
        args = self.__get_args_dict(testcoupler)
        checker = esgfpid.assistant.consistency.Checker(**args)
        checker._Checker__will_run_check = False # Switch the check off!

        # Run code to be tested:
        with self.assertRaises(ValueError) as raised:
            success = checker.data_consistency_check(['any', 'thing'])
        self.assertIn('No reason specified', raised.exception.message, 'Expected message did not arrive: "%s"' % raised.exception.message)


    def test_preparation_files_is_empty_list(self):

        # Prepare
        testcoupler = self.__make_patched_testcoupler()
        prev = []
        self.__patch_testcoupler_with_solr_mock(testcoupler, prev)
        args = self.__get_args_dict(testcoupler)

        # Run code to be tested:
        checker = esgfpid.assistant.consistency.Checker(**args)

        # Check result:
        self.assertIsInstance(checker, esgfpid.assistant.consistency.Checker,
            'Preparation failed.')
        self.assertFalse(checker.can_run_check(),
            'Without previous files, check should be disabled, but isn\'t.')

    def test_preparation_files_is_none(self):

        # Prepare
        testcoupler = self.__make_patched_testcoupler()
        self.__patch_testcoupler_with_solr_mock(testcoupler, 'NONE') # makes the mock return None. In reality, this should never happen!
        args = self.__get_args_dict(testcoupler)

        # Run code to be tested:
        checker = esgfpid.assistant.consistency.Checker(**args)

        # Check result:
        self.assertIsInstance(checker, esgfpid.assistant.consistency.Checker,
            'Preparation failed.')
        self.assertFalse(checker.can_run_check(),
            'Without previous files, check should be disabled, but isn\'t.')

    def test_preparation_error(self):

        # Prepare
        testcoupler = self.__make_patched_testcoupler()
        error_to_raise = esgfpid.exceptions.SolrError
        self.__patch_testcoupler_with_solr_mock(testcoupler, None, error_to_raise) # makes the mock raise an error
        args = self.__get_args_dict(testcoupler)

        # Run code to be tested:
        checker = esgfpid.assistant.consistency.Checker(**args)

        # Check result:
        self.assertIsInstance(checker, esgfpid.assistant.consistency.Checker,
            'Preparation failed.')
        self.assertFalse(checker.can_run_check(),
            'Without previous files, check should be disabled, but isn\'t.')

    def test_preparation_switched_off(self):

        # Prepare
        testcoupler = self.__make_patched_testcoupler(solr_off=True)
        args = self.__get_args_dict(testcoupler)

        # Run code to be tested:
        checker = esgfpid.assistant.consistency.Checker(**args)

        # Check result:
        self.assertIsInstance(checker, esgfpid.assistant.consistency.Checker,
            'Preparation failed.')
        self.assertFalse(checker.can_run_check(),
            'Without previous files, check should be disabled, but isn\'t.')
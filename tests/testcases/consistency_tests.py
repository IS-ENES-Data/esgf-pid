import unittest
import logging
from esgfpid.assistant.consistency import Checker as Checker
import esgfpid.exceptions # need SolrError

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
import resources.TESTVALUES as TESTHELPERS

'''
Unit tests for esgfpid.assistant.consistency

This module needs a coupler. A coupler has references to
solr and RabbitMQ.

In these tests, we use a real coupler, but mock the solr
module with mock.Mock() objects that return the desired
things.
Access to RabbitMQ is never used, so the rabbit module is
not mocked.
 
'''
class ConsistencyTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def test_preparation_ok(self):

        # Preparations: Make test coupler with patched solr.
        prev = ['abc', 'def']
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev)

        # Args for the consistency checker:
        args = TESTHELPERS.get_args_for_consistency_check()
        args['coupler'] = testcoupler

        # Run code to be tested:
        checker = Checker(**args)

        # Check result:
        self.assertIsInstance(checker, Checker,
            'Preparation failed.')
        self.assertTrue(checker.can_run_check(),
            'With previous files, check should be enabled, but isn\'t.')

    def test_check_ok(self):

        # Preparations: Make test coupler with patched solr.
        prev = ['abc', 'def']
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev)

        # Preparations: Make consistency checker.
        args = TESTHELPERS.get_args_for_consistency_check()
        args['coupler'] = testcoupler
        checker = Checker(**args)

        # Run code to be tested:
        success = checker.data_consistency_check(prev)

        # Check result:
        self.assertTrue(success, 'The check should pass.')

    def test_check_too_many_files(self):

        # Preparations: Make test coupler with patched solr.
        prev = ['abc', 'def']
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev)

        # Preparations: Make consistency checker.
        args = TESTHELPERS.get_args_for_consistency_check()
        args['coupler'] = testcoupler
        checker = Checker(**args)

        # Run code to be tested:
        too_many_files = prev+['ghi', 'jkl']
        success = checker.data_consistency_check(too_many_files)

        # Check result:
        self.assertFalse(success, 'The check should fail - too many files passed.')

    def test_check_too_few_files(self):

        # Preparations: Make test coupler with patched solr.
        prev = ['abc', 'def']
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev)

        # Preparations: Make consistency checker.
        args = TESTHELPERS.get_args_for_consistency_check()
        args['coupler'] = testcoupler
        checker = Checker(**args)

        # Run code to be tested:
        too_few_files = ['abc']
        success = checker.data_consistency_check(too_few_files)

        # Check result:
        self.assertFalse(success, 'The check should fail - too few files passed.')

    def test_check_different_files(self):

        # Preparations: Make test coupler with patched solr.
        prev = ['abc', 'def']
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev)

        # Preparations: Make consistency checker.
        args = TESTHELPERS.get_args_for_consistency_check()
        args['coupler'] = testcoupler
        checker = Checker(**args)

        # Run code to be tested:
        different_files = ['xyz', 'zyx']
        success = checker.data_consistency_check(different_files)

        # Check result:
        self.assertFalse(success, 'The check should fail - different files passed.')

    def test_check_no_files(self):

        # Preparations: Make test coupler with patched solr.
        prev = ['abc', 'def']
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev)

        # Preparations: Make consistency checker.
        args = TESTHELPERS.get_args_for_consistency_check()
        args['coupler'] = testcoupler
        checker = Checker(**args)

        # Run code to be tested:
        no_files = []
        with self.assertRaises(ValueError):
            success = checker.data_consistency_check(no_files)

    def test_check_try_if_disabled(self):

        # Preparations: Make test coupler with patched solr.
        prev = ['abc', 'def']
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev)

        # Preparations: Make consistency checker.
        args = TESTHELPERS.get_args_for_consistency_check()
        args['coupler'] = testcoupler
        checker = Checker(**args)
        # Switch it off!!!
        checker._Checker__will_run_check = False # Switch the check off!

        # Run code to be tested:
        with self.assertRaises(ValueError) as raised:
            success = checker.data_consistency_check(['any', 'thing'])
        self.assertIn('No reason specified', raised.exception.message, 'Expected message did not arrive: "%s"' % raised.exception.message)


    def test_preparation_files_is_empty_list(self):

        # Preparations: Make test coupler with patched solr.
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_empty_file_list(testcoupler)

        # Preparations: Make consistency checker.
        args = TESTHELPERS.get_args_for_consistency_check()
        args['coupler'] = testcoupler
        checker = Checker(**args)

        # Run code to be tested:
        checker = Checker(**args)

        # Check result:
        self.assertIsInstance(checker, Checker,
            'Preparation failed.')
        self.assertFalse(checker.can_run_check(),
            'Without previous files, check should be disabled, but isn\'t.')

    def test_preparation_files_is_none(self):

        # Preparations: Make test coupler with patched solr.
        prev = None
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_returns_previous_files(testcoupler, prev)

        # Preparations: Make consistency checker.
        args = TESTHELPERS.get_args_for_consistency_check()
        args['coupler'] = testcoupler
        checker = Checker(**args)

        # Check result:
        self.assertIsInstance(checker, Checker,
            'Preparation failed.')
        self.assertFalse(checker.can_run_check(),
            'Without previous files, check should be disabled, but isn\'t.')

    def test_preparation_error(self):

        # Preparations: Make test coupler with patched solr.
        testcoupler = TESTHELPERS.get_coupler()
        TESTHELPERS.patch_solr_raises_error(testcoupler, esgfpid.exceptions.SolrError)

        # Preparations: Args for consistency checker.
        args = TESTHELPERS.get_args_for_consistency_check()
        args['coupler'] = testcoupler

        # Run code to be tested:
        checker = Checker(**args)

        # Check result:
        self.assertIsInstance(checker, Checker,
            'Preparation failed.')
        self.assertFalse(checker.can_run_check(),
            'Without previous files, check should be disabled, but isn\'t.')

    def test_preparation_switched_off(self):

        # Preparations: Make test coupler with switched off solr.
        testcoupler = TESTHELPERS.get_coupler(solr_switched_off=True)

        # Preparations: Args for consistency checker.
        args = TESTHELPERS.get_args_for_consistency_check()
        args['coupler'] = testcoupler

        # Run code to be tested:
        checker = Checker(**args)

        # Check result:
        self.assertIsInstance(checker, Checker,
            'Preparation failed.')
        self.assertFalse(checker.can_run_check(),
            'Without previous files, check should be disabled, but isn\'t.')
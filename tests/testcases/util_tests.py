import unittest
import mock
import logging
import json
import sys
sys.path.append("..")
import esgfpid
import datetime
import uuid
import os

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

from resources.TESTVALUES import TESTVALUES as TESTVALUES


class UtilsTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')

    def tearDown(self):
        LOGGER.info('#############################')

    #
    # handleutils
    #

    def test_make_handle_from_drsid_and_versionnumber_ok(self):

        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        prefix = TESTVALUES['prefix']

        received_handle = esgfpid.utils.make_handle_from_drsid_and_versionnumber(
            drs_id=drs_id,
            version_number=version_number,
            prefix=prefix
        )

        expected_handle = 'hdl:'+prefix+'/'+TESTVALUES['suffix1']
        self.assertEquals(received_handle, expected_handle)

    #
    # argsutils
    #


    def test_check_presence_of_mandatory_args_ok(self):

        # Test variables
        args = {}
        args['mand1']='val1'
        args['mand2']='val2'
        args['mand3']='val3'
        args['opt1']='val1'
        args['opt2']='val2'
        args['opt3']='val3'
        mandatory_args = ['mand1','mand2','mand3']
  
        # Run code to be tested:

        all_mandatory_args_ok = esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

        # Check result:
        self.assertTrue(all_mandatory_args_ok, 'Checking mandatory args did not work.')

    def test_check_presence_of_mandatory_args_one_missing(self):

        # Test variables
        args = {}
        args['mand1']='val1'
        args['mand2']='val2'
        args['opt1']='val1'
        args['opt2']='val2'
        args['opt3']='val3'
        mandatory_args = ['mand1','mand2','mand3']
  
        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError)as raised:
            all_mandatory_args_ok = esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        expected_message = 'The arguments that were passed are not ok: Missing mandatory arguments: mand3'
        self.assertIn(expected_message, raised.exception.message,
            'Unexpected error message:\n%s\nExpected:\n%s' % (raised.exception.message, expected_message))

    def test_check_presence_of_mandatory_args_several_missing(self):

        # Test variables
        args = {}
        args['mand1']='val1'
        args['opt1']='val1'
        mandatory_args = ['mand1','mand2','mand3']
  
        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError)as raised:
            all_mandatory_args_ok = esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        expected_message = 'The arguments that were passed are not ok: Missing mandatory arguments: mand2, mand3'
        self.assertIn(expected_message, raised.exception.message,
            'Unexpected error message:\n%s\nExpected:\n%s' % (raised.exception.message, expected_message))

    def test_find_additional_args_found_ok(self):

        # Test variables:
        mandatory_args = ['mand1','mand2','mand3']
        optional_args = ['opt1','opt2','opt3']
        all_args = dict(
            mand1=100,
            opt2=100,
            another=10000,
            mand3=100,
            opt1=100,
            mand2=100,
            opt3=100
        )

        # Run code to be tested:
        received = esgfpid.utils.find_additional_args(all_args, (mandatory_args+optional_args))

        # Check result:
        expected = {'another':10000}
        self.assertEqual(received, expected)

    def test_find_additional_args_none_found(self):

        # Test variables:
        mandatory_args = ['mand1','mand2','mand3']
        optional_args = ['opt1','opt2','opt3']
        all_args = dict(
            mand1=100,
            opt2=100,
            mand3=100,
            opt1=100,
            mand2=100,
            opt3=100
        )

        # Run code to be tested:
        received = esgfpid.utils.find_additional_args(all_args, (mandatory_args+optional_args))

        # Check result:
        expected = {}
        self.assertEqual(received, expected)

    def test_check_noneness_of_mandatory_args_none(self):
        mandatory_args = ['mand1','mand2']

        all_args = dict(
            mand1=100,
            opt2=100,
            another=10000,
            mand2=None,
        )

        with self.assertRaises(esgfpid.exceptions.ArgumentError) as raised:
            esgfpid.utils.check_noneness_of_mandatory_args(all_args, mandatory_args)
        self.assertIn('These arguments are None: mand2', raised.exception.message)

    def test_check_noneness_of_mandatory_args_ok(self):
        mandatory_args = ['mand1','mand2']

        all_args = dict(
            mand1=100,
            opt2=100,
            another=10000,
            mand2=100,
        )

        success = esgfpid.utils.check_noneness_of_mandatory_args(all_args, mandatory_args)
        self.assertTrue(success)

    def test_add_missing_optional_args_with_value_none_ok(self): 
        mandatory_args = ['mand1','mand2']
        optional_args = ['opt1','opt2']

        all_args = dict(
            mand1=100,
            opt2=100,
            another=10000,
            mand2=100,
        )
        esgfpid.utils.add_missing_optional_args_with_value_none(all_args, optional_args)
        self.assertIn('opt1', all_args)
        self.assertIn('opt2', all_args)
        self.assertTrue(all_args['opt1'] is None)
        self.assertTrue(all_args['opt2'] is not None)


    def test_add_missing_optional_args_with_value_none_not_needed(self): 
        mandatory_args = ['mand1','mand2']
        optional_args = ['opt1','opt2']

        all_args = dict(
            mand1=100,
            opt1=100,
            opt2=100,
            another=10000,
            mand2=100,
        )
        esgfpid.utils.add_missing_optional_args_with_value_none(all_args, optional_args)
        self.assertIn('opt1', all_args)
        self.assertIn('opt2', all_args)
        self.assertTrue(all_args['opt1'] is not None)
        self.assertTrue(all_args['opt2'] is not None)

    #
    # timeutils
    #

    def test_get_now_utc(self):
        received = esgfpid.utils.get_now_utc()
        self.assertIsInstance(received, datetime.datetime, 'Unexpected time type: %s' % type(received))

    def test_get_now_utc_as_string(self):
        received = esgfpid.utils.get_now_utc_as_formatted_string() #2016-07-07T15:25:18.258224+00:00
        self.assertIn('2016-', received, 'Unexpected time: %s' % received)
        self.assertIn('T', received, 'Unexpected time: %s' % received)
        self.assertIn('+00:00', received, 'Unexpected time: %s' % received)

    #
    # miscutils - get_boolean
    #

    def test_get_boolean_true(self):
        self.assertTrue(esgfpid.utils.get_boolean(True))
        self.assertTrue(esgfpid.utils.get_boolean('True'))
        self.assertTrue(esgfpid.utils.get_boolean('true'))

    def test_get_boolean_false(self):
        self.assertFalse(esgfpid.utils.get_boolean(False))
        self.assertFalse(esgfpid.utils.get_boolean('False'))
        self.assertFalse(esgfpid.utils.get_boolean('false'))

    def test_get_boolean_string_wrong(self):
        with self.assertRaises(ValueError):
            self.assertFalse(esgfpid.utils.get_boolean('maybe'))

    def test_get_boolean_number_wrong(self):
        with self.assertRaises(ValueError):
            self.assertFalse(esgfpid.utils.get_boolean(123))

    #
    # miscutils - directory helper
    #

    def test_ensure_dir_exists_alreadythere(self):
        ''' The method should not do anything if the directory exists already.'''

        # Make a test dir:
        workingdir = os.getcwd()
        mydir = workingdir+'/'+str(uuid.uuid4())
        os.mkdir(mydir)
        if not os.path.isdir(mydir):
            raise ValueError('We just created this dir, it should exist: %s' % mydir)

        # Run method:
        esgfpid.utils.ensure_directory_exists(mydir)

        # Check result:
        self.assertTrue(os.path.isdir(mydir), 'Directory does not exist. This is strange.')

        # Delete again:
        os.rmdir(mydir)
        if os.path.isdir(mydir):
          raise ValueError('Could not remove test dir after testing: %s' % mydir)

    def test_ensure_dir_exists_notthereyet(self):
        ''' The method should create the directory, as it does not exist.'''

        # Make a name of test dir:
        workingdir = os.getcwd()
        mydir = workingdir+'/'+str(uuid.uuid4())
        if os.path.isdir(mydir):
            raise ValueError('This directory should not exist: %s' % mydir)

        # Run method:
        esgfpid.utils.ensure_directory_exists(mydir)

        # Check result:
        self.assertTrue(os.path.isdir(mydir), 'Directory does not exist. This is strange.')

        # Delete again:
        os.rmdir(mydir)
        if os.path.isdir(mydir):
          raise ValueError('Could not remove test dir after testing: %s' % mydir)

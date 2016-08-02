import unittest
import mock
import logging
import json
import sys
sys.path.append("..")
import esgfpid
import datetime
import uuid
import esgfpid.utils
import os

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

from resources.TESTVALUES import TESTVALUES as TESTVALUES

class LoggerMock(object):

    def __init__(self):
        self.info_messages = []
        self.debug_messages = []
        self.error_messages = []
        self.warn_messages = []

    def info(self, msg, *args, **kwargs):
        msgf = msg % args
        self.info_messages.append(msgf)

    def debug(self, msg, *args, **kwargs):
        msgf = msg % args
        self.debug_messages.append(msgf)

    def warn(self, msg, *args, **kwargs):
        msgf = msg % args
        self.warn_messages.append(msgf)

    def error(self, msg, *args, **kwargs):
        msgf = msg % args
        self.error_messages.append(msgf)

class UtilsLoggingTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ##########')

    def tearDown(self):
        LOGGER.info('#############################')

    #
    # Test whether args are passed ok!
    #

    @mock.patch('esgfpid.defaults')
    def test_log_info_pass_args_kwargs(self, defaults_patch):
        ''' Test if info messages stay info when flag is not set! '''

        # Test variables
        logger = LoggerMock()

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = False # <-- This is tested
        defaults_patch.LOG_SHOW_TO_INFO = 999 # irrelevant here
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.loginfo(logger, 'barbaz %s %i', 'a', 1, show=True)

        # Check results:
        expected_info = 'barbaz a 1'
        self.assertIn(expected_info, logger.info_messages, 'Received messages: %s' % logger.info_messages)

    #
    # Test whether correct levels are reached
    #

    @mock.patch('esgfpid.defaults')
    def test_log_info_normal(self, defaults_patch):
        ''' Test if info messages stay info when flag is not set! '''

        # Test variables
        logger = LoggerMock()

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = False # <-- This is tested
        defaults_patch.LOG_SHOW_TO_INFO = 999 # irrelevant here
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.loginfo(logger, 'barbaz')

        # Check results:
        self.assertIn('barbaz', logger.info_messages, 'Nope')

    @mock.patch('esgfpid.defaults')
    def test_log_info_to_debug(self, defaults_patch):
        ''' Test if info messages are made debug when flag is set! '''

        # Test variables
        logger = LoggerMock()

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = True # <-- This is tested
        defaults_patch.LOG_SHOW_TO_INFO = 999 # irrelevant here
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.loginfo(logger, 'barbaz')

        # Check results:
        self.assertIn('barbaz', logger.debug_messages, 'Nope')

    @mock.patch('esgfpid.defaults')
    def test_log_debug_normal(self, defaults_patch):
        ''' Test normal debug messages! '''

        # Test variables
        logger = LoggerMock()

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = 999 # irrelevant here
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.logdebug(logger, 'foofoo')

        # Check results:
        self.assertIn('foofoo', logger.debug_messages, 'Nope')

    @mock.patch('esgfpid.defaults')
    def test_log_debug_show_show(self, defaults_patch):
        ''' Test if debug messages are made info when show flag is set! '''

        # Test variables
        logger = LoggerMock()
        show = True

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = True # this is relevant!
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.logdebug(logger, 'foofoo', show=show)

        # Check results:
        self.assertIn('foofoo', logger.info_messages, 'Nope')


    @mock.patch('esgfpid.defaults')
    def test_log_debug_show_notshow(self, defaults_patch):
        '''
        Test if debug messages are kept DEBUG info
        if show flag is set for the message,
        but the general show flag is false.
         '''

        # Test variables
        logger = LoggerMock()
        show = True

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = False # this is relevant!
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.logdebug(logger, 'foofoo', show=show)

        # Check results:
        self.assertIn('foofoo', logger.debug_messages, 'Nope')

    @mock.patch('esgfpid.defaults')
    def test_log_trace_normal_ignored(self, defaults_patch):
        ''' Test normal trace messages! '''

        # Test variables
        logger = LoggerMock()

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = 999 # irrelevant here
        defaults_patch.LOG_TRACE_TO_DEBUG = False #  this is relevant!
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.logtrace(logger, 'superdetail')

        # Check results:
        self.assertEquals(logger.info_messages, [], 'Received info: %s' % (logger.info_messages))
        self.assertEquals(logger.debug_messages, [], 'Received debug: %s' % (logger.debug_messages))

    @mock.patch('esgfpid.defaults')
    def test_log_trace_to_debug(self, defaults_patch):
        ''' Test trace messages that are shown as DEBUG '''

        # Test variables
        logger = LoggerMock()

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = 999 # irrelevant here
        defaults_patch.LOG_TRACE_TO_DEBUG = True #  this is relevant!
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.logtrace(logger, 'superdetail')

        # Check results:
        expected_debug = '[trace] superdetail'
        self.assertIn(expected_debug, logger.debug_messages, 'Received debug: %s' % (logger.debug_messages))
        self.assertEquals(logger.info_messages, [], 'Received info: %s' % (logger.info_messages))

    @mock.patch('esgfpid.defaults')
    def test_log_trace_show_show(self, defaults_patch):
        ''' Test trace messages that are shown as DEBUG and then as INFO'''

        # Test variables
        logger = LoggerMock()
        show = True

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = True # this is relevant!
        defaults_patch.LOG_TRACE_TO_DEBUG = True # this is relevant
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.logtrace(logger, 'superdetail', show=show)

        # Check results:
        expected_info = '[trace] superdetail'
        self.assertIn(expected_info, logger.info_messages, 'Received info: %s' % (logger.info_messages))
        self.assertEquals(logger.debug_messages, [], 'Received debug: %s' % (logger.debug_messages))

    @mock.patch('esgfpid.defaults')
    def test_log_warn_normal(self, defaults_patch):
        ''' Test if info messages stay info when flag is not set! '''

        # Test variables
        logger = LoggerMock()

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = 999 # irrelevant here
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.logwarn(logger, 'danger')

        # Check results:
        self.assertIn('danger', logger.warn_messages, 'Warn messages: %s' % logger.warn_messages)

    @mock.patch('esgfpid.defaults')
    def test_log_warn_show_show(self, defaults_patch):
        ''' Test if info messages stay info when flag is not set! '''

        # Test variables
        logger = LoggerMock()
        show = True

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = True # <- This!
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.logwarn(logger, 'danger', show=show)

        # Check results:
        self.assertIn('[WARN] danger', logger.info_messages, 'Info messages: %s' % logger.info_messages)

    @mock.patch('esgfpid.defaults')
    def test_log_error_normal(self, defaults_patch):
        ''' Test if info messages stay info when flag is not set! '''

        # Test variables
        logger = LoggerMock()

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = 999 # irrelevant here
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.logerror(logger, 'danger')

        # Check results:
        self.assertIn('danger', logger.error_messages, 'Error messages: %s' % logger.error_messages)

    @mock.patch('esgfpid.defaults')
    def test_log_error_show_show(self, defaults_patch):
        ''' Test if info messages stay info when flag is not set! '''

        # Test variables
        logger = LoggerMock()
        show = True

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = True # <- This!
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        esgfpid.utils.logerror(logger, 'danger', show=show)

        # Check results:
        self.assertIn('[ERROR] danger', logger.info_messages, 'Info messages: %s' % logger.info_messages)


    #
    # Logging every xth message
    #

    @mock.patch('esgfpid.defaults')
    def test_log_every_x_times_normal_debug_ok(self, defaults_patch):
        '''
        Check ifmessage is logged every 10th time, and the first time.
        Messages should be printed as debug.
        '''

        # Test variables
        logger = LoggerMock()
        counter = 0
        x = 10
        msg = 'Foobar'

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = 999  # irrelevant here
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        for i in xrange(35):
            counter += 1
            esgfpid.utils.log_every_x_times(logger, counter, x, (msg+str(counter)))

        # Check result
        received_messages = ', '.join(logger.debug_messages)
        expected_messages = 'Foobar1 (counter 1), Foobar10 (counter 10), Foobar20 (counter 20), Foobar30 (counter 30)'
        self.assertEquals(expected_messages, received_messages, 'Received messages: %s' % received_messages)

    @mock.patch('esgfpid.defaults')
    def test_log_every_x_times_show_show(self, defaults_patch):
        '''
        Check if message is logged every 10th time, and the first time.
        Messages should be printed as info, as the show flag is set.
        '''

        # Test variables
        logger = LoggerMock()
        counter = 0
        x = 10
        show = True
        msg = 'Foobar'

        # Prepare flags
        defaults_patch.LOG_INFO_TO_DEBUG = 999 # irrelevant here
        defaults_patch.LOG_SHOW_TO_INFO = True # <- This is relevant!
        defaults_patch.LOG_TRACE_TO_DEBUG = 999 # irrelevant here
        reload(esgfpid.utils)

        # Run code to be tested:
        for i in xrange(35):
            counter += 1
            esgfpid.utils.log_every_x_times(logger, counter, x, (msg+str(counter)), show=show)

        # Check result
        received_messages = ', '.join(logger.info_messages)
        expected_messages = 'Foobar1 (counter 1), Foobar10 (counter 10), Foobar20 (counter 20), Foobar30 (counter 30)'
        self.assertEquals(expected_messages, received_messages, 'Received messages: %s' % received_messages)

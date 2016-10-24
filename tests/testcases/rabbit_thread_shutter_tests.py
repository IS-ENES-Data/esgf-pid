import unittest
import mock
import logging
import copy
import os
import esgfpid.rabbit.asynchronous.thread_shutter
from esgfpid.rabbit.asynchronous.exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

UNCONFIRMED_TAGS = [1,2,3,4]
UNCONFIRMED_MESSAGES = {'1':'foo1', '2':'foo2', '3':'foo3', '4':'foo4'}

#
# Create mock ups
#

class MockFeeder(object):

    def __init__(self, num_unpublished=0, reduce_at_every_call=False):
        self.num = num_unpublished
        self.reduce = reduce_at_every_call

    def get_num_unpublished(self):
        num = self.num
        if num < 0:
            num = 0
        if self.reduce:
            self.num -= 1
        return num

class MockThread(object):
    def __init__(self):
        self._connection = mock.MagicMock()
        self.ERROR_CODE_CONNECTION_CLOSED_BY_USER = 999
        self.ERROR_TEXT_CONNECTION_FORCE_CLOSED= 'foo'
        self.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN = 'bar'
    def _make_permanently_closed_by_user(self):
        pass

class ThreadShutterTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ########## (%s)' % os.path.basename(__file__))

    def tearDown(self):
        LOGGER.info('#############################')

    def make_shutter(self, feeder=None):

        # Mocked:
        self.thread = MockThread()
        self.feeder = feeder or MockFeeder()
        self.statemachine = esgfpid.rabbit.asynchronous.thread_statemachine.StateMachine()
        self.confirmer = esgfpid.rabbit.asynchronous.thread_confirmer.ConfirmReactor()
        shutter = esgfpid.rabbit.asynchronous.thread_shutter.ShutDowner(
            self.thread,
            self.statemachine,
            self.confirmer,
            self.feeder)
        self.statemachine._StateMachine__state = self.statemachine._StateMachine__IS_AVAILABLE
        return shutter

    def add_leftovers_to_confirmer(self):
        self.confirmer._ConfirmReactor__unconfirmed_delivery_tags = copy.copy(UNCONFIRMED_TAGS)
        self.confirmer._ConfirmReactor__unconfirmed_messages_dict = copy.copy(UNCONFIRMED_MESSAGES)

    # Tests

    #
    # Force finish
    #

    def test_force_finish_no_leftovers_ok(self):

        # Preparation:
        shutter = self.make_shutter()

        # Run code to be tested:
        shutter.force_finish()

        # Check result state:
        state = self.statemachine._StateMachine__state
        exp = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(self.confirmer.get_num_unconfirmed(), 0)
        self.assertEquals(self.feeder.get_num_unpublished(), 0)

    def test_force_finish_leftovers_ok(self):

        # Preparation:
        shutter = self.make_shutter()
        self.add_leftovers_to_confirmer()

        # Run code to be tested:
        shutter.force_finish()

        # Check result state:
        state = self.statemachine._StateMachine__state
        exp = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(self.confirmer.get_num_unconfirmed(), 4)
        self.assertEquals(self.feeder.get_num_unpublished(), 0)

    #
    # Gentle finish
    #

    def test_gently_finish_no_leftovers_ok(self):

        # Preparation:
        shutter = self.make_shutter()

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = self.statemachine._StateMachine__state
        exp = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(self.confirmer.get_num_unconfirmed(), 0)
        self.assertEquals(self.feeder.get_num_unpublished(), 0)

    def test_gently_finish_with_unpublished_messages_ok(self):

        # Preparation:
        shutter = self.make_shutter(MockFeeder(3, True))

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = self.statemachine._StateMachine__state
        exp = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(self.confirmer.get_num_unconfirmed(), 0)
        self.assertEquals(self.feeder.get_num_unpublished(), 0)

    def test_gently_finish_with_unconfirmed_messages_ok(self):
        print("\nThis test takes time, as it calls the finish_gently method while not all messages are done.")

        # Preparation:
        shutter = self.make_shutter()
        self.add_leftovers_to_confirmer()

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = self.statemachine._StateMachine__state
        exp = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(self.confirmer.get_num_unconfirmed(), 4)
        self.assertEquals(self.feeder.get_num_unpublished(), 0)

    def test_gently_finish_with_leftovers_not_progressing_1(self):

        # Preparation:
        shutter = self.make_shutter(MockFeeder(4))
        self.add_leftovers_to_confirmer()
        self.statemachine._StateMachine__state = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = self.statemachine._StateMachine__state
        exp = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(self.confirmer.get_num_unconfirmed(), 4)
        self.assertEquals(self.feeder.get_num_unpublished(), 4)

    def test_gently_finish_with_leftovers_not_progressing_2(self):

        # Preparation:
        shutter = self.make_shutter(MockFeeder(4))
        self.statemachine._StateMachine__state = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.statemachine.closed_by_publisher = True

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = self.statemachine._StateMachine__state
        exp = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(self.confirmer.get_num_unconfirmed(), 0)
        self.assertEquals(self.feeder.get_num_unpublished(), 4)

    def test_gently_finish_with_leftovers_not_progressing_3(self):

        # Preparation:
        shutter = self.make_shutter(MockFeeder(4))
        self.statemachine._StateMachine__state = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.statemachine.could_not_connect = True

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = self.statemachine._StateMachine__state
        exp = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(self.confirmer.get_num_unconfirmed(), 0)
        self.assertEquals(self.feeder.get_num_unpublished(), 4)

    def test_gently_finish_with_leftovers_timeout(self):
        print("\nThis test takes time, as it calls the finish_gently method while not all messages are done.")

        # Preparation:
        shutter = self.make_shutter(MockFeeder(4))

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = self.statemachine._StateMachine__state
        exp = self.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(self.confirmer.get_num_unconfirmed(), 0)
        self.assertEquals(self.feeder.get_num_unpublished(), 4)
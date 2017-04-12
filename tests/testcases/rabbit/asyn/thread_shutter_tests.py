import unittest
import mock
import logging
import esgfpid.rabbit.asynchronous.thread_shutter
from esgfpid.rabbit.asynchronous.exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
import resources.TESTVALUES as TESTHELPERS

class ThreadShutterTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def make_shutter(self):

        # Mocked:
        thread = TESTHELPERS.get_thread_mock4()
        statemachine = esgfpid.rabbit.asynchronous.thread_statemachine.StateMachine()
        statemachine._StateMachine__state = statemachine._StateMachine__IS_AVAILABLE

        shutter = esgfpid.rabbit.asynchronous.thread_shutter.ShutDowner(
            thread,
            statemachine)
        return shutter

    #
    # Force finish
    #

    '''
    Simple force-close.
    No messages are left.
    '''
    def test_force_finish_no_leftovers_ok(self):

        # Preparation:
        shutter = self.make_shutter()

        # Run code to be tested:
        shutter.force_finish()

        # Check result state:
        state = shutter.statemachine._StateMachine__state
        exp = shutter.statemachine._StateMachine__FORCE_FINISHED
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(shutter.thread.get_num_unconfirmed(), 0)
        self.assertEquals(shutter.thread.get_num_unpublished(), 0)
        # Check if connection was closed:
        shutter.thread._connection.close.assert_called()

    '''
    Force-close when some messages are still unpublished/unconfirmed.
    They stay unpublished/unconfirmed.
    '''
    def test_force_finish_leftovers_ok(self):

        # Preparation:
        shutter = self.make_shutter()
        shutter.thread.num_unconfirmed = 4
        shutter.thread.num_unpublished = 8

        # Run code to be tested:
        shutter.force_finish()

        # Check result state:
        state = shutter.statemachine._StateMachine__state
        exp = shutter.statemachine._StateMachine__FORCE_FINISHED
        # Check leftovers:
        self.assertEquals(shutter.thread.get_num_unconfirmed(), 4)
        self.assertEquals(shutter.thread.get_num_unpublished(), 8)
        self.assertFalse(shutter.thread.add_event_publish_message.called)
        # Check if connection was closed:
        shutter.thread._connection.close.assert_called()

    '''
    Simple force-close.
    No messages are left.
    '''
    def test_force_finish_was_closed(self):

        # Preparation:
        shutter = self.make_shutter()
        shutter.thread._connection.is_closed = True
        shutter.thread._connection.is_open = False

        # Run code to be tested:
        shutter.force_finish()

        # Check result state:
        state = shutter.statemachine._StateMachine__state
        exp = shutter.statemachine._StateMachine__FORCE_FINISHED
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(shutter.thread.get_num_unconfirmed(), 0)
        self.assertEquals(shutter.thread.get_num_unpublished(), 0)
        # Check if ioloop was closed:
        shutter.thread.make_permanently_closed_by_user.assert_called_with()

    #
    # Gentle finish
    #

    def test_gently_finish_no_leftovers_ok(self):

        # Preparation:
        shutter = self.make_shutter()

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = shutter.statemachine._StateMachine__state
        exp = shutter.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)
        # Check leftovers:
        self.assertEquals(shutter.thread.get_num_unconfirmed(), 0)
        self.assertEquals(shutter.thread.get_num_unpublished(), 0)
        self.assertFalse(shutter.thread.add_event_publish_message.called)
        # Check if connection was closed:
        shutter.thread._connection.close.assert_called()
    
    def test_gently_finish_with_unpublished_messages_ok(self):

        # Preparation:
        shutter = self.make_shutter()
        shutter.thread.num_unpublished = 4

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = shutter.statemachine._StateMachine__state
        exp = shutter.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)

        # Check leftovers:
        self.assertEquals(shutter.thread.get_num_unconfirmed(), 0)
        self.assertEquals(shutter.thread.get_num_unpublished(), 0)
        self.assertTrue(shutter.thread.add_event_publish_message.called)
        # Check if connection was closed:
        shutter.thread._connection.close.assert_called()

    def test_gently_finish_with_unconfirmed_messages_ok(self):

        # Preparation:
        shutter = self.make_shutter()
        shutter.thread.num_unconfirmed = 4
        shutter.thread.fake_confirms = True
        shutter.thread.confirm_rate = 0.22

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = shutter.statemachine._StateMachine__state
        exp = shutter.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)

        # Check leftovers:
        self.assertEquals(shutter.thread.get_num_unconfirmed(), 0)
        self.assertEquals(shutter.thread.get_num_unpublished(), 0)
        # Check if connection was closed:
        shutter.thread._connection.close.assert_called()

    def test_gently_finish_wait_too_long(self):

        # Preparation:
        shutter = self.make_shutter()
        shutter.thread.num_unconfirmed = 100
        shutter.thread.fake_confirms = True
        shutter.thread.confirm_rate = 0.22

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = shutter.statemachine._StateMachine__state
        exp = shutter.statemachine._StateMachine__FORCE_FINISHED
        self.assertEquals(state, exp)

        # Check leftovers:
        self.assertTrue(shutter.thread.get_num_unconfirmed()>0)
        # Check if connection was closed:
        shutter.thread._connection.close.assert_called()

    def test_gently_finish_with_leftovers_not_progressing_1(self):

        # Preparation:
        shutter = self.make_shutter()
        shutter.thread.num_unpublished = 100
        shutter.thread.num_unconfirmed = 100
        shutter.thread.fake_confirms = False
        shutter.statemachine.set_to_permanently_unavailable()
        shutter.statemachine.detail_could_not_connect = True

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = shutter.statemachine._StateMachine__state
        exp = shutter.statemachine._StateMachine__FORCE_FINISHED
        self.assertEquals(state, exp)

        # Check leftovers:
        self.assertEquals(shutter.thread.get_num_unconfirmed(), 100)
        self.assertEquals(shutter.thread.get_num_unpublished(), 100)
        # Check if connection was closed:
        shutter.thread._connection.close.assert_called()

    '''
    Same as before, but different reason.
    '''
    def test_gently_finish_with_leftovers_not_progressing_2(self):

        # Preparation:
        shutter = self.make_shutter()
        shutter.thread.num_unpublished = 100
        shutter.thread.num_unconfirmed = 100
        shutter.thread.fake_confirms = False
        shutter.statemachine.set_to_permanently_unavailable()
        shutter.statemachine.detail_authentication_exception = True

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = shutter.statemachine._StateMachine__state
        exp = shutter.statemachine._StateMachine__FORCE_FINISHED
        self.assertEquals(state, exp)

        # Check leftovers:
        self.assertEquals(shutter.thread.get_num_unconfirmed(), 100)
        self.assertEquals(shutter.thread.get_num_unpublished(), 100)
        # Check if connection was closed:
        shutter.thread._connection.close.assert_called()

    '''
    Same as before, but different reason.
    '''
    def test_gently_finish_with_leftovers_not_progressing_3(self):

        # Preparation:
        shutter = self.make_shutter()
        shutter.thread.num_unpublished = 100
        shutter.thread.num_unconfirmed = 100
        shutter.thread.fake_confirms = False
        shutter.statemachine.set_to_permanently_unavailable()
        shutter.statemachine.set_detail_closed_by_publisher()

        # Run code to be tested:
        shutter.finish_gently()

        # Check result state:
        state = shutter.statemachine._StateMachine__state
        exp = shutter.statemachine._StateMachine__FORCE_FINISHED
        self.assertEquals(state, exp)

        # Check leftovers:
        self.assertEquals(shutter.thread.get_num_unconfirmed(), 100)
        self.assertEquals(shutter.thread.get_num_unpublished(), 100)
        # Check if connection was closed:
        shutter.thread._connection.close.assert_called()

    def test_not_continuing(self):

        # Preparation:
        shutter = self.make_shutter()
        
        # Run code to be tested
        shutter.continue_gently_closing_if_applicable()

        # Check results
        self.assertFalse(shutter.thread._connection.add_timeout.called)

        # Check if connection was not closed yet:
        # (because it is not applicable!)
        shutter.thread._connection.close.assert_not_called()

    def test_continuing_trigger(self):

        # Preparation:
        shutter = self.make_shutter()
        shutter._ShutDowner__is_in_process_of_gently_closing = True
        shutter.recursive_decision_about_closing = mock.MagicMock() # to be able to check if it was called
        
        # Run code to be tested
        shutter.continue_gently_closing_if_applicable()
        self.assertTrue(shutter.thread._connection.add_timeout.called)
        self.assertTrue(shutter.recursive_decision_about_closing.called)
        # Check if connection was closed:
        # (Cannot be closed, as we mock the method that would lead to closing)
        shutter.thread._connection.close.assert_not_called()

    def test_continuing_whole(self):

        # Preparation:
        shutter = self.make_shutter()
        shutter.thread.num_unpublished = 100
        shutter.thread.num_unconfirmed = 100
        shutter.thread.fake_confirms = False

        # Patch the shutter method to interrupt it:
        real_method = shutter.recursive_decision_about_closing
        shutter.recursive_decision_about_closing = mock.MagicMock() # to be able to check if it was called
        
        # Start a gentle close down, which is interrupted by the patch:
        shutter.finish_gently()

        # Shutting is interrupted, so messages were not sent:
        self.assertFalse(shutter.thread.add_event_publish_message.called)
        self.assertEquals(shutter.thread.num_unconfirmed, 100)
        self.assertEquals(shutter.thread.num_unpublished, 100)
        # The mock was called, instead of the real method (this is the interrupting):
        self.assertTrue(shutter.recursive_decision_about_closing.called)

        # Restore the old method
        shutter.recursive_decision_about_closing = real_method
        shutter.thread.fake_confirms = True

        # Run code to be tested: Continue shutting:
        shutter.continue_gently_closing_if_applicable()

        # Check if the shutting really was continued
        self.assertTrue(shutter.thread.add_event_publish_message.called)

        # Check if messages were sent now:
        self.assertTrue(shutter.thread.num_unconfirmed <= 99)
        self.assertTrue(shutter.thread.num_unpublished <= 99)
        # Check if connection was closed:
        shutter.thread._connection.close.assert_called()

    #
    # Safety finish
    #


    def test_safety_finish_ok(self):

        # Preparation:
        shutter = self.make_shutter()
        shutter.thread.num_unconfirmed = 4
        shutter.thread.num_unpublished = 5

        # Run code to be tested:
        shutter.safety_finish()

        # Check result state:
        state = shutter.statemachine._StateMachine__state
        exp = shutter.statemachine._StateMachine__PERMANENTLY_UNAVAILABLE
        self.assertEquals(state, exp)

        # Check leftovers:
        self.assertEquals(shutter.thread.get_num_unconfirmed(), 4)
        self.assertEquals(shutter.thread.get_num_unpublished(), 5)
        # Check if connection was closed:
        shutter.thread._connection.close.assert_called()

import logging
import esgfpid.defaults as defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from esgfpid.utils import get_now_utc_as_formatted_string as get_now_utc_as_formatted_string
import time

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
This class contains the algorithm for closing down
gently, which means that it does try to send the last
pending messages and to receive the last confirms for
a while before it closes down the connection.

The algorithm is started by the "execute()" method.

It ends by one of the three "__close_because_xxx()"
methods, which trigger the actual closing of the
connection.

'''
class GentleFinish(object):

    def __init__(self, shutter, statemachine):
        self.__shutter = shutter
        self.__statemachine = statemachine

        '''
        To count how many times we've iterated during
        the iterative, recursive check for pending messages.
        '''
        self.__close_decision_iterations = 0

        '''
        To see, in case of a reconnect, if the module was in
        the process of gently finishing, so we can add that
        to the new ioloop.
        Otherwise, the gently-closing is lost during a reconnect.
        '''
        self.__is_in_process_of_gently_closing = False

    '''
    This method is only called from the shutter module.
    '''
    def execute(self):
        #self.__statemachine.asked_to_closed_by_publisher = True # TODO

        # Make sure no more messages are accepted from publisher # TODO
        # while publishes/confirms are still accepted:
        #if self.__statemachine.is_available_for_client_publishes():
        #    self.__statemachine.set_to_wanting_to_stop()

        # Inform user
        if self.__are_any_messages_pending():
            wait_seconds = defaults.RABBIT_ASYN_FINISH_WAIT_SECONDS
            max_waits = defaults.RABBIT_ASYN_FINISH_MAX_TRIES
            loginfo(LOGGER, 'Preparing to close PID module. Some messages are pending. Maximum waiting time: %i seconds. (%s)', wait_seconds*max_waits, get_now_utc_as_formatted_string())
        else:
            loginfo(LOGGER, 'Closing PID module. No pending messages. (%s)', get_now_utc_as_formatted_string())

        # Go through decision tree (close or wait for pending messages)
        self.__close_decision_iterations = 1
        self.__is_in_process_of_gently_closing = True
        self.recursive_decision_about_closing()
        # This iteratively checks if all messages are published+confirmed.
        # If not, it waits and then rechecks, up to a maximum number of iterations.
        # The main thread waits for this by using a threading.Event.

    ''' Called by builder (via thread), so close-events are not lost if a new ioloop is started.'''
    def continue_gently_closing_if_applicable(self):
        if self.__is_in_process_of_gently_closing:
            logdebug(LOGGER, 'Continue gentle shutdown even after reconnect (iteration %i)...', self.__close_decision_iterations)
            if self.thread._connection is not None:
                wait_seconds = defaults.RABBIT_ASYN_FINISH_WAIT_SECONDS
                self.thread._connection.add_timeout(wait_seconds, self.recursive_decision_about_closing)
            else:
                logerror(LOGGER, 'Connection was None when trying to wait for pending messages (after reconnect). Synchronization error between threads!')

    # One entry point to the algorithm:

    '''
    This needs to be public, as it is added to the ioloop           This has to be public, ... TODO
    as an event.
    '''
    def recursive_decision_about_closing(self):
        logdebug(LOGGER, 'Gentle finish (iteration %i): Deciding about whether we can close the thread or not...', self.__close_decision_iterations)
        iteration = self.__close_decision_iterations
        if self.__are_any_messages_pending():
            self.__inform_about_pending_messages()
            self.__decide_what_to_do_about_pending_messages(iteration)
        else:
            self.__close_because_all_done(iteration)

    def __decide_what_to_do_about_pending_messages(self, iteration):
        logdebug(LOGGER, 'Gentle finish (iteration %i): Decide what to do about the pending messages...', self.__close_decision_iterations)
        if self.__have_we_waited_enough_now(iteration):
            self.__close_because_waited_long_enough()
        elif self.__module_is_not_progressing_anymore():
            self.__close_because_no_point_in_waiting()
        else:
            self.__wait_some_more_and_redecide(iteration)

    def __inform_about_pending_messages(self):
        msg = self.__get_string_about_pending_messages()
        if msg is not None:
            logdebug(LOGGER, 'Pending messages: %s.', msg)

    # Decision rules:

    def __have_we_waited_enough_now(self, iteration):
        logdebug(LOGGER, 'Gentle finish (iteration %i): Check if the rabbit thread has waited long enough...', self.__close_decision_iterations)

        wait_seconds = defaults.RABBIT_ASYN_FINISH_WAIT_SECONDS
        max_waits = defaults.RABBIT_ASYN_FINISH_MAX_TRIES

        tried = iteration
        waited = iteration-1

        # Logging:
        logdebug(LOGGER, 'Gentle finish (iteration %i): At this point we have tried %i times and waited %i/%i times (%i seconds)', self.__close_decision_iterations, tried, waited, max_waits, waited*wait_seconds)
        log_every_x_seconds = 2
        if ((waited*wait_seconds)%log_every_x_seconds==0 or waited>=max_waits):
            msg = self.__get_string_about_pending_messages()
            loginfo(LOGGER, 'Still pending: %s messages... (waited %.1f/%.1f seconds)', msg, waited*wait_seconds, max_waits*wait_seconds)
        
        # Return:
        if waited >= max_waits:
            logdebug(LOGGER, 'Gentle finish (iteration %i): The rabbit thread has waited long enough for pending messages at close down.', self.__close_decision_iterations)
            return True
        logdebug(LOGGER, 'Gentle finish (iteration %i): We should wait a little more for pending messages.', self.__close_decision_iterations)
        return False

    def __are_any_messages_pending(self):
        logdebug(LOGGER, 'Gentle finish (iteration %i): Checking for any pending messages...', self.__close_decision_iterations)
        sent_done = self.__check_all_were_sent()
        confirmed_done = self.__check_all_were_confirmed()
        if sent_done and confirmed_done:
            logdebug(LOGGER, 'Gentle finish (iteration %i): No more pending messages.', self.__close_decision_iterations)
            return False # none pending
        logdebug(LOGGER, 'Gentle finish (iteration %i): Some pending messages left.', self.__close_decision_iterations)
        return True # some are pending

    def __check_all_were_sent(self):
        approx_number_of_messages_in_queue = self.thread.get_num_unpublished()
        if approx_number_of_messages_in_queue == 0:
            return True
        else:
            return False

    def __check_all_were_confirmed(self):
        if self.thread.get_num_unconfirmed() == 0:
            return True
        else:
            return False


    def __module_is_not_progressing_anymore(self):
        if self.__statemachine.is_PERMANENTLY_UNAVAILABLE(): # TODO Do I have to check anything else?
            logdebug(LOGGER, 'Gentle finish (iteration %i): The rabbit thread is not active anymore, so we might as well close it.', self.__close_decision_iterations)
            return True
        return False

    def __wait_some_more_and_redecide(self, iteration):
        wait_seconds = defaults.RABBIT_ASYN_FINISH_WAIT_SECONDS
        logdebug(LOGGER, 'Gentle finish (iteration %i): Waiting some more for pending messages...', self.__close_decision_iterations)
        # Instead of time.sleep(), add an event to the thread's ioloop
        self.__close_decision_iterations += 1
        if self.thread._connection is not None:
            self.thread._connection.add_timeout(wait_seconds, self.recursive_decision_about_closing)
            self.__is_in_process_of_gently_closing = True
            # Problem: If a reconnect occurs after this, this event will be lost.
            # I cannot retrieve if from the ioloop and pass it to the new one.
            # So, during a reconnection, we check if gently-finish was running,
            # and add a new timeout to the new ioloop, using
            # "continue_gently_closing_if_applicable()".
            # This may mess up the amount of time the gently-finish takes, though.
            # TODO Maybe one day it is possible to transfer events from one ioloop to
            # another?
        else:
            logerror(LOGGER, 'Connection was None when trying to wait for pending messages. Synchronization error between threads!')


    #
    # Three possible exits from the algorithm
    #

    def __close_because_all_done(self, iteration):
        logdebug(LOGGER, 'Gentle finish (iteration %i): All messages sent and confirmed in %ith try (waited and rechecked %i times).', self.__close_decision_iterations, iteration, iteration-1)
        loginfo(LOGGER, 'All messages sent and confirmed. Closing.')
        self.__shutter.normal_close_down()
        self.__tell_publisher_to_stop_waiting_for_gentle_finish()

    def __close_because_no_point_in_waiting(self):

        # Logging, depending on why we closed...
        logdebug(LOGGER, 'Gentle finish (iteration %i): Closing, as there is no point in waiting any longer.', self.__close_decision_iterations)
        if self.__statemachine.get_detail_closed_by_publisher():
            logwarn(LOGGER, 'Not waiting for pending messages: No connection to server (previously closed by user).')
        elif self.__statemachine.detail_could_not_connect:
            logwarn(LOGGER, 'Not waiting for pending messages: No connection to server (unable to connect).')
        elif self.__statemachine.detail_authentication_exception:
            logwarn(LOGGER, 'Not waiting for pending messages: No connection to server (authentication exception).')
        else:
            logwarn(LOGGER, 'Not waiting for pending messages: No connection to server (unsure why).')

        # Actual close
        self.__shutter.abrupt_close_down('Force finish as we are not sending the messages anyway.')
        self.__tell_publisher_to_stop_waiting_for_gentle_finish()

    def __close_because_waited_long_enough(self):
        logdebug(LOGGER, 'We have waited long enough. Now closing by force.')
        self.__shutter.abrupt_close_down('Force finish as normal waiting period in normal finish is over.')
        self.__tell_publisher_to_stop_waiting_for_gentle_finish()

    #
    # Helpers
    #

    def __tell_publisher_to_stop_waiting_for_gentle_finish(self):
        logdebug(LOGGER, 'Main thread does not need to wait anymore. (%s).', get_now_utc_as_formatted_string())

        # This avoids that the last iteration is redone
        # and redone upon reconnection, as after reconnection,
        # if this is True, the algorithm is entered again.
        self.__is_in_process_of_gently_closing = False

        # This releases the event that blocks the main thread
        # until the gentle finish is done.
        self.thread.tell_publisher_to_stop_waiting_for_gentle_finish()

    def __get_string_about_pending_messages(self):
        unsent = self.thread.get_num_unpublished()
        unconfirmed = self.thread.get_num_unconfirmed()
        if unsent + unconfirmed > 0:
            msg = '%i (%i unsent, %i unconfirmed)' % ((unsent+unconfirmed), unsent, unconfirmed)
            return msg
        else:
            return None
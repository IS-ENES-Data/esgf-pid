import logging
import esgfpid.defaults as defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from esgfpid.utils import get_now_utc_as_formatted_string as get_now_utc_as_formatted_string
import time

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class ShutDowner(object):

    def __init__(self, thread, statemachine):
        self.thread = thread
        self.statemachine = statemachine

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

    ####################
    ### Force finish ###
    ####################

    def force_finish(self):
        try:
            return self.__force_finish('Forced finish from outside the thread.')
        except Exception as e:
            logwarn(LOGGER, 'Error in shutter.force_finish(): %s: %s', e.__class__.__name__, e.message)
            raise e

    #####################
    ### Gentle finish ###
    #####################

    def finish_gently(self):
        try:
            return self.__finish_gently()
        except Exception as e:
            logwarn(LOGGER, 'Error in shutter.finish_gently(): %s: %s', e.__class__.__name__, e.message)
            raise e

    def __finish_gently(self):
        # Called directly from outside the thread!

        # No more messages can arrive from publisher (because
        # the main thread blocks), but publishes/confirms are still
        # accepted.

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
            # Make sure the messages can be sent, in case some events
            # were lost during reconnecting or something...
            num_unpub = self.thread.get_num_unpublished()
            logdebug(LOGGER, 'Triggering %i publish events...' % num_unpub)
            for i in xrange(int(1.1*num_unpub)):
                self.thread.add_event_publish_message()
            # Now wait some more...
            self.__wait_some_more_and_redecide(iteration)

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
        if self.statemachine.is_PERMANENTLY_UNAVAILABLE() or self.statemachine.is_FORCE_FINISHED():
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

    
    def __tell_publisher_to_stop_waiting_for_gentle_finish(self):
        logdebug(LOGGER, 'Main thread does not need to wait anymore. (%s).', get_now_utc_as_formatted_string())

        # This avoids that the last iteration is redone
        # and redone upon reconnection, as after reconnection,
        # if this is True, the algorithm is entered again.
        self.__is_in_process_of_gently_closing = False

        # This releases the event that blocks the main thread
        # until the gentle finish is done.
        self.thread.tell_publisher_to_stop_waiting_for_gentle_finish()

    def __close_because_all_done(self, iteration):
        logdebug(LOGGER, 'Gentle finish (iteration %i): All messages sent and confirmed in %ith try (waited and rechecked %i times).', self.__close_decision_iterations, iteration, iteration-1)
        loginfo(LOGGER, 'All messages sent and confirmed. Closing.')
        self.__normal_finish()
        self.__tell_publisher_to_stop_waiting_for_gentle_finish()

    def __close_because_no_point_in_waiting(self):

        # Logging, depending on why we closed...
        logdebug(LOGGER, 'Gentle finish (iteration %i): Closing, as there is no point in waiting any longer.', self.__close_decision_iterations)
        if self.statemachine.get_detail_closed_by_publisher():
            logwarn(LOGGER, 'Not waiting for pending messages: No connection to server (previously closed by user).')
        elif self.statemachine.detail_could_not_connect:
            logwarn(LOGGER, 'Not waiting for pending messages: No connection to server (unable to connect).')
        else:
            logwarn(LOGGER, 'Not waiting for pending messages: No connection to server (unsure why).')

        # Actual close
        self.__force_finish('Force finish as we are not sending the messages anyway.')
        self.__tell_publisher_to_stop_waiting_for_gentle_finish()

    def __close_because_waited_long_enough(self):
        logdebug(LOGGER, 'We have waited long enough. Now closing by force.')
        self.__force_finish('Force finish as normal waiting period in normal finish is over.')
        self.__tell_publisher_to_stop_waiting_for_gentle_finish()

    def __inform_about_pending_messages(self):
        msg = self.__get_string_about_pending_messages()
        if msg is not None:
            logdebug(LOGGER, 'Pending messages: %s.', msg)

    def __get_string_about_pending_messages(self):
        unsent = self.thread.get_num_unpublished()
        unconfirmed = self.thread.get_num_unconfirmed()
        if unsent + unconfirmed > 0:
            msg = '%i (%i unsent, %i unconfirmed)' % ((unsent+unconfirmed), unsent, unconfirmed)
            return msg
        else:
            return None

    #################
    ### Finishing ###
    #################

    def __force_finish(self, msg):
        logdebug(LOGGER, 'Force finishing, reason: %s.', msg)
        self.statemachine.set_to_force_finished()
        reply_code = self.thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER
        reply_text = msg+' '+self.thread.ERROR_TEXT_CONNECTION_FORCE_CLOSED
        self.__close_down(reply_code, reply_text)
        self.__inform_about_state_at_shutdown()

    def __normal_finish(self, msg='Normal finish.'):
        reply_code = self.thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER
        reply_text = msg+' '+self.thread.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN
        self.__close_down(reply_code, reply_text)
        self.__inform_about_state_at_shutdown()

    def safety_finish(self, msg='Safety finish'):
        reply_code = self.thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER
        reply_text = msg+' '+self.thread.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN
        self.__close_down(reply_code, reply_text)

    def __close_down(self, reply_code, reply_text):
        # Important, so the connection does not get reopened by 
        # the on_connection_closed callback.
        # This should only be called by one of the finish methods.

        # Make sure the main thread does not continue blocking
        # as it believes that we're still looking for pending messages:
        self.__tell_publisher_to_stop_waiting_for_gentle_finish()

        # Change the state of the state machine:
        self.statemachine.set_to_permanently_unavailable()
        self.statemachine.set_detail_closed_by_publisher()

        # Close connection
        try:
            if self.thread._connection is not None:

                if self.thread._connection.is_closed or self.thread._connection.is_closing:
                    logdebug(LOGGER, 'Connection is closed or closing.')
                    # If connection is already closed, the on_connection_close is not
                    # called, so the ioloop continues, possibly waiting for reconnect.
                    # So we need to prevent reconnects or other events. As long
                    # as ioloop runs, thread cannot be finished/joined.
                    self.thread.make_permanently_closed_by_user()
                elif self.thread._connection.is_open:
                    logdebug(LOGGER, 'Connection is open. Closing now. This will trigger the RabbitMQ callbacks.')
                    self.thread._connection.close(reply_code=reply_code, reply_text=reply_text)
                    # "If there are any open channels, it will attempt to close them prior to fully disconnecting." (pika docs)
            else:
                logerror(LOGGER, 'Connection was None when trying to close. Synchronization error between threads!')

        except AttributeError as e:
            logdebug(LOGGER, 'AttributeError from pika during connection closedown (%s: %s)', e.__class__.__name__, e.message)

    def __inform_about_state_at_shutdown(self):
        unsent = self.thread.get_num_unpublished()
        unconfirmed = self.thread.get_num_unconfirmed()
        if unsent + unconfirmed > 0:
            logwarn(LOGGER, 
                'At close down: %i pending messages (%i unpublished messages, %i unconfirmed messages).',
                (unsent+unconfirmed), unsent, unconfirmed)
        else:
            loginfo(LOGGER, 'After close down: All messages were published and confirmed.')

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

    ####################
    ### Force finish ###
    ####################

    def force_finish(self):
        self.__force_finish('Forced finish from unknown source')

    #####################
    ### Gentle finish ###
    #####################

    def finish_gently(self):
        # Called directly from outside the thread!
        #self.statemachine.asked_to_closed_by_publisher = True # TODO

        # Make sure no more messages are accepted from publisher # TODO
        # while publishes/confirms are still accepted:
        #if self.statemachine.is_available_for_client_publishes():
        #    self.statemachine.set_to_wanting_to_stop()

        # Inform user
        if self.__are_any_messages_pending():
            wait_seconds = defaults.RABBIT_ASYN_FINISH_WAIT_SECONDS
            max_waits = defaults.RABBIT_ASYN_FINISH_MAX_TRIES
            loginfo(LOGGER, 'Preparing to close PID module. Some messages are pending. Maximum waiting time: %i seconds. (%s)', wait_seconds*max_waits, get_now_utc_as_formatted_string())
        else:
            loginfo(LOGGER, 'Closing PID module. No pending messages. (%s)', get_now_utc_as_formatted_string())

        # Go through decision tree (close or wait for pending messages)
        self.__close_decision_iterations = 1
        self.recursive_decision_about_closing()
        # This iteratively checks if all messages are published+confirmed.
        # If not, it waits and then rechecks, up to a maximum number of iterations.
        # THe main thread waits for this by using a threading.Event.


    def recursive_decision_about_closing(self):
        logdebug(LOGGER, 'Gentle finish: Deciding about whether we can close the thread or not... %i', self.__close_decision_iterations)
        iteration = self.__close_decision_iterations
        if self.__are_any_messages_pending():
            self.__inform_about_pending_messages()
            self.__decide_what_to_do_about_pending_messages(iteration)
        else:
            self.__close_because_all_done(iteration)

    def __decide_what_to_do_about_pending_messages(self, iteration):
        logdebug(LOGGER, 'Gentle finish: Decide what to do about the pending messages...')
        if self.__have_we_waited_enough_now(iteration):
            self.__close_because_waited_long_enough()
        elif self.__module_is_not_progressing_anymore():
            self.__close_because_no_point_in_waiting()
        else:
            self.__wait_some_more_and_redecide(iteration)

    # Decision rules:

    def __have_we_waited_enough_now(self, iteration):
        logdebug(LOGGER, 'Gentle finish: Check if the rabbit thread has waited long enough...')

        wait_seconds = defaults.RABBIT_ASYN_FINISH_WAIT_SECONDS
        max_waits = defaults.RABBIT_ASYN_FINISH_MAX_TRIES

        tried = iteration
        waited = iteration-1

        # Logging:
        logdebug(LOGGER, 'Gentle finish: At this point we have tried %i times and waited %i/%i times (%i seconds)', tried, waited, max_waits, waited*wait_seconds)
        log_every_x_seconds = 2
        if ((waited*wait_seconds)%log_every_x_seconds==0 or waited>=max_waits):
            msg = self.__get_string_about_pending_messages()
            loginfo(LOGGER, 'Still waiting for %s pending messages... (waited %.1f/%.1f seconds)', msg, waited*wait_seconds, max_waits*wait_seconds)
        
        # Return:
        if waited >= max_waits:
            logdebug(LOGGER, 'Gentle finish: The rabbit thread has waited long enough for pending messages at close down.')
            return True
        logdebug(LOGGER, 'Gentle finish: We need to wait a little more for pending messages.')
        return False

    def __are_any_messages_pending(self):
        logdebug(LOGGER, 'Gentle finish: Checking for any pending messages...')
        sent_done = self.__check_all_were_sent()
        confirmed_done = self.__check_all_were_confirmed()
        if sent_done and confirmed_done:
            logdebug(LOGGER, 'Gentle finish: No more pending messages.')
            return False # none pending
        logdebug(LOGGER, 'Gentle finish: Some pending messages left.')
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
        if self.statemachine.is_PERMANENTLY_UNAVAILABLE(): # TODO Do I have to check anything else?
            logdebug(LOGGER, 'Gentle finish: The rabbit thread is not active anymore, so we might as well close it.')
            return True
        return False

    def __wait_some_more_and_redecide(self, iteration):
        wait_seconds = defaults.RABBIT_ASYN_FINISH_WAIT_SECONDS
        logdebug(LOGGER, 'Gentle finish: Waiting some more for pending messages...')
        # Instead of time.sleep(), add an event to the thread's ioloop
        self.__close_decision_iterations += 1
        if self.thread._connection is not None:
            self.thread._connection.add_timeout(wait_seconds, self.recursive_decision_about_closing)
        else:
            logerror(LOGGER, 'Gentle finish: Connection is None when we tried to close it.')
    
    def __tell_publisher_to_stop_waiting(self):
        logdebug(LOGGER, 'Main thread does not need to wait anymore. (%s).', get_now_utc_as_formatted_string())
        self.thread.tell_publisher_to_stop_waiting()

    def __close_because_all_done(self, iteration):
        logdebug(LOGGER, 'Gentle finish: All messages sent and confirmed in %ith try (waited and rechecked %i times).', iteration, iteration-1)
        loginfo(LOGGER, 'All messages sent and confirmed. Closing.')
        self.__normal_finish()
        self.__tell_publisher_to_stop_waiting()

    def __close_because_no_point_in_waiting(self):
        logdebug(LOGGER, 'Gentle finish: Closing, as there is no point in waiting any longer.')

        if self.statemachine.detail_closed_by_publisher:
            logwarn(LOGGER, 'Not waiting for pending messages: No connection to server (previously closed by user).')
            self.__force_finish('Force finish as we are not sending the messages anyway.')
            self.__tell_publisher_to_stop_waiting()

        elif self.statemachine.detail_could_not_connect:
            logwarn(LOGGER, 'Not waiting for pending messages: No connection to server (unable to connect).')
            self.__force_finish('Force finish as we are not sending the messages anyway.') # TODO does this actually make sense here?
            self.__tell_publisher_to_stop_waiting()

        else:
            logwarn(LOGGER, 'Not waiting for pending messages: No connection to server (unsure why).')
            self.__force_finish('Force finish as we are not sending the messages anyway.') # TODO does this actually make sense here?
            self.__tell_publisher_to_stop_waiting()

    def __close_because_waited_long_enough(self):
        logdebug(LOGGER, 'We have waited long enough. Now closing by force.')
        self.__force_finish('Force finish as normal waiting period in normal finish is over.')
        self.__tell_publisher_to_stop_waiting()

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
        self.__tell_publisher_to_stop_waiting()

        # Change the state of the state machine:
        self.statemachine.set_to_permanently_unavailable()
        self.statemachine.detail_closed_by_publisher = True

        # Close connection
        try:
            if self.thread._connection is not None:

                if self.thread._connection.is_closed or self.thread._connection.is_closing:
                    logdebug(LOGGER, 'Connection is closed or closing.')
                    # If connection is already closed, the on_connection_close is not
                    # called, so the ioloop continues, possibly waiting for reconnect.
                    # So we need to prevent reconnects or other events. As long
                    # as ioloop runs, thread cannot be finished/joined.
                    self.thread._make_permanently_closed_by_user()
                elif self.thread._connection.is_open:
                    logdebug(LOGGER, 'Connection is open. Closing now. This will trigger the RabbitMQ callbacks.')
                    self.thread._connection.close(reply_code=reply_code, reply_text=reply_text)
                    # "If there are any open channels, it will attempt to close them prior to fully disconnecting." (pika docs)

        except AttributeError as e:
            logdebug(LOGGER, 'AttributeError from pika during connection closedown (%s)' % e.message)

    def __inform_about_state_at_shutdown(self):
        unsent = self.thread.get_num_unpublished()
        unconfirmed = self.thread.get_num_unconfirmed()
        if unsent + unconfirmed > 0:
            logwarn(LOGGER, 
                'At close down: %i pending messages (%i unpublished messages, %i unconfirmed messages).',
                (unsent+unconfirmed), unsent, unconfirmed)
        else:
            loginfo(LOGGER, 'After close down: All messages were published and confirmed.')

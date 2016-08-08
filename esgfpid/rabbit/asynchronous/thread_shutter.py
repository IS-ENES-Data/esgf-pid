import logging
import esgfpid.defaults as defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
import time

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class ShutDowner(object):

    def __init__(self, thread, statemachine, confirmer, feeder):
        self.thread = thread
        self.feeder = feeder
        self.confirmer = confirmer
        self.statemachine = statemachine

    ####################
    ### Force finish ###
    ####################

    def force_finish(self, msg='Forced finish from unknown source'):
        # Called directly from outside the thread! Or if gentle-finish did not work.
        self.statemachine.asked_to_closed_by_publisher = True
        logwarn(LOGGER,
            'Forced close down of message sending module. Will not wait for any pending messages, if any.')
        self.statemachine.set_to_wanting_to_stop()
        self.__force_finish(msg)

    #####################
    ### Gentle finish ###
    #####################

    def finish_gently(self):
        # Called directly from outside the thread!
        self.statemachine.asked_to_closed_by_publisher = True

        # Make sure no more messages are accepted from publisher
        # while publishes/confirms are still accepted:
        if self.statemachine.is_available_for_client_publishes():
            self.statemachine.set_to_wanting_to_stop()

        # Inform user
        msg = 'Module received finish command.'
        logdebug(LOGGER, msg)
        if self.__are_any_messages_pending():
            wait_seconds = defaults.RABBIT_ASYN_FINISH_WAIT_SECONDS
            max_waits = defaults.RABBIT_ASYN_FINISH_MAX_TRIES
            loginfo(LOGGER, ' Max waiting time for pending messages: %i.', wait_seconds*max_waits)

        # Go through decision tree (close or wait for pending messages)
        self.__recursive_decision_about_closing(iteration=1)

        # To be sure:
        logdebug(LOGGER, 'Safety finish.')
        self.safety_finish()

    def __recursive_decision_about_closing(self, iteration):
        if self.__are_any_messages_pending():
            self.__inform_about_pending_messages()
            self.__decide_what_to_do_about_pending_messages(iteration)
        else:
            self.__close_because_all_done(iteration)

    def __decide_what_to_do_about_pending_messages(self, iteration):
        if self.__have_we_waited_enough_now(iteration):
            self.__close_because_waited_long_enough()
        elif self.__module_is_not_progressing_anymore():
            self.__close_because_no_point_in_waiting()
        else:
            self.__wait_some_more_and_redecide(iteration)

    # Decision rules:

    def __have_we_waited_enough_now(self, iteration):

        wait_seconds = defaults.RABBIT_ASYN_FINISH_WAIT_SECONDS
        max_waits = defaults.RABBIT_ASYN_FINISH_MAX_TRIES

        tried = iteration
        waited = iteration-1
        logtrace(
            LOGGER,
            'At this point we have tried %i times and waited %i/%i times (%i seconds)',
            tried, waited, max_waits, waited*wait_seconds
        )
        if waited >= max_waits:
            return True
        return False

    def __are_any_messages_pending(self):
        sent_done = self.__check_all_were_sent()
        confirmed_done = self.__check_all_were_confirmed()
        if sent_done and confirmed_done:
            return False # none pending
        return True # some are pending

    def __check_all_were_sent(self):
        approx_number_of_messages_in_queue = self.feeder.get_num_unpublished()
        if approx_number_of_messages_in_queue == 0:
            return True
        else:
            return False

    def __check_all_were_confirmed(self):
        if self.confirmer.get_num_unconfirmed() == 0:
            return True
        else:
            return False

    def __module_is_not_progressing_anymore(self):
        return self.statemachine.is_permanently_unavailable()
        # TODO Do I have to check anything else?

    # Actions:

    def __wait_some_more_and_redecide(self, iteration):
        wait_seconds = defaults.RABBIT_ASYN_FINISH_WAIT_SECONDS
        logdebug(LOGGER, 'Waiting for pending messages.')
        time.sleep(wait_seconds)
        self.__recursive_decision_about_closing(iteration+1)

    def __close_because_all_done(self, iteration):
        logdebug(LOGGER, 'All messages sent and confirmed in %ith try (waited and rechecked %i times).', iteration, iteration-1)
        loginfo(LOGGER, 'All messages sent and confirmed. Closing.')
        self.__normal_finish()

    def __close_because_no_point_in_waiting(self):
        #self.__inform_about_pending_messages()
        msg = 'Not waiting for pending messages: No connection to server'

        if self.statemachine.closed_by_publisher:
            logwarn(LOGGER, msg+' (previously closed by user).')
            self.force_finish('Force finish as we are not sending the messages anyway.')

        elif self.statemachine.could_not_connect:
            logwarn(LOGGER, msg+' (unable to connect).')
            self.force_finish('Force finish as we are not sending the messages anyway.') # TODO does this actually make sense here?

        else:
            logwarn(LOGGER, msg+' (unsure why).')
            self.force_finish('Force finish as we are not sending the messages anyway.') # TODO does this actually make sense here?

    def __close_because_waited_long_enough(self):
        logwarn(LOGGER, 'We have waited long enough. Now closing by force.')
        self.force_finish('Force finish as normal waiting period in normal finish is over.')

    def __inform_about_pending_messages(self):
        unsent = self.feeder.get_num_unpublished()
        unconfirmed = self.confirmer.get_num_unconfirmed()
        if unsent + unconfirmed > 0:
            logdebug(LOGGER, 'Pending messages: %i (%i unsent, %i unconfirmed).',
                (unsent+unconfirmed),
                unsent,
                unconfirmed)

    #################
    ### Finishing ###
    #################

    def __force_finish(self, msg):
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
        # Important, so it does not get reopened by the callback.
        # This should only be called by one of the finish methods.

        # This changes the state of the state machine.
        self.statemachine.set_to_permanently_unavailable()
        self.statemachine.closed_by_publisher = True

        # Close pika objects
        #if self._channel is not None:
        #    self._channel.close(reply_code=reply_code, reply_text=reply_text)
        try:
            if self.thread._connection is not None:
                self.thread._connection.close(reply_code=reply_code, reply_text=reply_text)
                # "If there are any open channels, it will attempt to close them prior to fully disconnecting." (pika docs)
        except AttributeError as e:
            logdebug(LOGGER, 'AttributeError from pika during connection closedown (%s)' % e.message)

    def __inform_about_state_at_shutdown(self):
        unsent = self.feeder.get_num_unpublished()
        unconfirmed = self.confirmer.get_num_unconfirmed()
        if unsent + unconfirmed > 0:
            logwarn(LOGGER, 
                'At close down: %i pending messages (%i unpublished messages, %i unconfirmed messages).',
                (unsent+unconfirmed), unsent, unconfirmed)
        else:
            logdebug(LOGGER, 'At close down: All messages were published and confirmed already.')

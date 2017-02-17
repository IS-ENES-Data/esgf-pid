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

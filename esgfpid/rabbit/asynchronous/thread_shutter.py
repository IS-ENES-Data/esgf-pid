import logging
import esgfpid.defaults as defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from esgfpid.utils import get_now_utc_as_formatted_string as get_now_utc_as_formatted_string
import time
from esgfpid.rabbit.asynchronous.gentle_finish import GentleFinish

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class ShutDowner(object):

    def __init__(self, thread, statemachine):
        self.thread = thread
        self.statemachine = statemachine

        self.__gentle_finish = GentleFinish(self, self.statemachine)

    ###########
    ### API ###
    ###########

    '''
    Force-finishes the thread.
    This can be called from outside the thread, i.e. from
    the publisher directly. In this case, there is no argument.     
            
    This can also be called at the end of gentle-finish, in case
    we cannot wait for any messages. Then, there is a message       
    passed to explain it.       
    '''     
    def force_finish(self, msg=None):       
        if msg is None:     
            msg = 'Forced finish from outside the thread.'      
        try:
            logdebug(LOGGER, 'Force finishing, reason: %s.', msg)
            self.abrupt_close_down(msg)       
        except Exception as e:
            logwarn(LOGGER, 'Error in shutter.force_finish(): %s: %s', e.__class__.__name__, e.message)
            raise e

    def finish_gently(self):
        try:
            return self.__finish_gently.execute()
        except Exception as e:
            logwarn(LOGGER, 'Error in shutter.finish_gently(): %s: %s', e.__class__.__name__, e.message)
            raise e

    def continue_gently_closing_if_applicable(self):        
        self.__finish_gently.continue_gently_closing_if_applicable()

    def safety_finish(self):
        self.__safety_close_down()

    #
    # Different ways of closing
    #

    '''
    Only to be called from inside here and from the
    gently-closing algorithm.
    '''
    def __normal_close_down(self, msg='Normal finish.'):
        reply_code = self.thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER
        reply_text = msg+' '+self.thread.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN
        self.__close_down(reply_code, reply_text)
        self.__inform_about_state_at_shutdown()


    def __safety_close_down(self, msg='Safety finish'):
        reply_code = self.thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER
        reply_text = msg+' '+self.thread.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN
        self.__close_down(reply_code, reply_text)

    '''
    Only to be called from inside here and from the
    gently-closing algorithm.
    '''
    def __abrupt_close_down(self, msg):
        logdebug(LOGGER, 'Force finishing, reason: %s.', msg)
        reply_code = self.thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER
        reply_text = msg+' '+self.thread.ERROR_TEXT_CONNECTION_FORCE_CLOSED
        self.__close_down(reply_code, reply_text)
        self.__inform_about_state_at_shutdown()

    '''
    This triggers the close of the connection to RabbitMQ from
    the client side (connection.close), which then triggers the
    on_connection_closed callbacks.

    In case the connection is already closed or closing, the callback
    is not called. In this case, we directly trigger the important stuff
    that the on_connection_closed callback would do otherwise.

    This is the only point in the whole library that closes 
    the connection to RabbitMQ from the client side. All client-
    side closes must go through here.

    This method should only be called by one of the close methods,
    which make sure we sent the correct codes along. These codes
    are needed for the callbacks to know how to treat the closing
    event. For example, a closing event desired by the user should
    NOT lead to a reconnection attempt.
    '''
    def __close_down(self, reply_code, reply_text):
        # Important, so the connection does not get reopened by 
        # the on_connection_closed callback.
        # This should only be called by one of the finish methods.

        # Make sure the main thread does not continue blocking
        # as it believes that we're still looking for pending messages:
        self.__tell_publisher_to_stop_waiting_for_gentle_finish()
        # TODO This should be called by the gentle-method.

        # Change the state of the state machine:
        self.statemachine.set_to_permanently_unavailable()
        self.statemachine.set_detail_closed_by_publisher() # todo might be error too

        # Close connection
        try:
            if self.thread._connection is None:
                # This should never happen. How could a close-down happen before we even started?
                logerror(LOGGER, 'Connection was None when trying to close. Synchronization error between threads!')

            else:
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
                    # (pika docs) - so we don't need to manually close the channel.
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

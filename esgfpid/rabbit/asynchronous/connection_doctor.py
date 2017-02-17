import logging
import esgfpid.defaults as defaults
import esgfpid.rabbit.connparams
from .exceptions import PIDServerException
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
This module only handles connection/channel errors.

It has two callbacks:
    on_connection_error()
    on_channel_closed()

On connection errors, we reconnect, i.e. we connect
to the next available host. If the maximum number of
attempts is reached, we give up TODO AND THROW EXCEPTION!

If the channel gets closed and it is NOT part of a
user-induced normal closedown, we identify the problem
and try to remedy.

Normal shutdowns are handles in the shutter module.


'''
class ConnectionDoctor(object):
    
    def __init__(self, builder, thread, statemachine, shutter, nodemanager):

        '''
        We need to add the reconnect-event to the ioloop.
        We need to stop the ioloop before we reconnect.

        Use thread to forward method calls:

        reset_delivery_number() --> forwarded to feeder
        reset_unconfirmed_messages_and_delivery_tags() --> forward to confirmer
        get_unconfirmed_messages_as_list_copy_during_lifetime() --> forward to confirmer
        send_many_messages() --> forward to asynchronous module
        '''
        self.thread = thread


        '''
        The builder module is needed for reconnecting (i.e.
        making an entirely new connection), and for reopening
        a channel (using the same connection).

        These are the two ways that errors are remedied, so
        the flow goes back to the normal module.

        The builder module is also used for setting the fallback
        exchange name. TODO is this the best way?
        '''
        self.builder = builder


        '''
        We need to set states (before we trigger something),
        and check states (if the beheaviour
        depends on the state).
        '''
        self.statemachine = statemachine


        '''
        We need the shutter to close the connection in case the
        channel had an error. We could just close the connection
        here, but it is better to have all connection-closes in
        one place.
        '''
        self.shutter = shutter

        '''
        The node manager keeps all the info about the RabbitMQ nodes,
        e.g. URLs, usernames, passwords.
        '''
        self.__nodemanager = nodemanager

        ''' To count how many times we have tried to reconnect to the same RabbitMQ URL.'''
        self.__reconnect_counter = 0


    '''
    Called from builder module, on_channel_open callback.
    Because when a connection succeeded, the next time there is an
    error and we need to reconnect, we restart the whole algorithm.
    '''
    def reset_reconnection_counter(self):
        self.__reconnect_counter = 0

    '''
    If the connection to RabbitMQ failed, there is various
    things that may happen:
    (1) If there is other RabbitMQ urls, it will try to connect 
        to one of these.
    (2) If there is no other URLs, it will try to reconnect to this
        one after a short waiting time.
    (3) If the maximum number of reconnection tries is reached, it
        gives up.
    '''
    def on_connection_error(self, connection, msg):

        oldhost = self.__nodemanager.get_connection_parameters().host
        loginfo(LOGGER, 'Failed connection to RabbitMQ at %s. Reason: %s.', oldhost, msg)

        
        # If there is alternative URLs, try one of them:
        if self.__nodemanager.has_more_urls():
            logdebug(LOGGER, 'Connection failure: %s fallback URLs left to try.', self.__nodemanager.get_num_left_urls())
            self.__nodemanager.set_next_host()
            newhost = self.__nodemanager.get_connection_parameters().host
            loginfo(LOGGER, 'Connection failure: Trying to connect (now) to %s.', newhost)
            reopen_seconds = 0
            self.__wait_and_trigger_reconnection(connection, reopen_seconds)


        # If there is no URLs, reset the node manager to
        # start at the first nodes again...
        else:
            self.__reconnect_counter += 1;
            if self.__reconnect_counter <= defaults.RABBIT_ASYN_RECONNECTION_MAX_TRIES:
                reopen_seconds = defaults.RABBIT_ASYN_RECONNECTION_SECONDS
                logdebug(LOGGER, 'Connection failure: Failed connecting to all hosts. Waiting %s seconds and starting over.', reopen_seconds)
                self.__nodemanager.reset_nodes()
                newhost = self.__nodemanager.get_connection_parameters().host
                loginfo(LOGGER, 'Connection failure: Trying to connect (in %s seconds) to %s.', reopen_seconds, newhost)
                self.__wait_and_trigger_reconnection(connection, reopen_seconds)

            # Give up after so many tries...
            else:
                self.statemachine.set_to_permanently_unavailable()
                self.statemachine.detail_could_not_connect = True
                max_tries = defaults.RABBIT_ASYN_RECONNECTION_MAX_TRIES
                errormsg = ('Permanently failed to connect to RabbitMQ. Tried all hosts %s times. Giving up. No PID requests will be sent.' % max_tries)
                logwarn(LOGGER, errormsg)
                raise PIDServerException(errormsg)


    '''
    This triggers a reconnection to whatever host is stored in
    self.__nodemanager.get_connection_parameters().host at the moment of reconnection.

    If it is called to reconnect to the same host, it is better
    to wait some seconds.

    If it is used to connect to the next host, there is no point
    in waiting.
    '''
    def __wait_and_trigger_reconnection(self, connection, wait_seconds):

        # Do not reconnect if the library was permanently closed.
        if self.statemachine.is_PERMANENTLY_UNAVAILABLE():
            logdebug(LOGGER, 'No more reconnection, as the library was permanently closed.')
            # No need to do anything else. Whoever set the state
            # to permanently unavailable also handled the necessary
            # close down steps.

        # Otherwise, do reconnect.
        else:
            self.statemachine.set_to_waiting_to_be_available()
            loginfo(LOGGER, 'Trying to reconnect to RabbitMQ in %s seconds.', wait_seconds)
            connection.add_timeout(wait_seconds, self.reconnect)
            logtrace(LOGGER, 'Reconnect event added to connection %s (not to %s)', connection, self.thread._connection)
            # TODO DIFFERENCE BETWEEN TWO CONNECTION OBJECTS?!?!

    '''
    Reconnecting creates a completely new connection.
    If we reconnect, we need to reset message number,
    delivery tag etc.

    We need to prepare to republish the yet-unconfirmed
    messages.

    Then we need to stop the old connection's ioloop.
    The reconnection will create a new connection object
    and this will have its own ioloop.

    '''
    def reconnect(self):
        logdebug(LOGGER, 'Reconnecting...')

        # We need to reset delivery tags, unconfirmed messages,
        # republish the unconfirmed, ...
        self.__prepare_channel_reopen('Reconnect')
        
        # This is the old connection ioloop instance, stop its ioloop
        logdebug(LOGGER, 'Reconnect: Stopping ioloop of connection %s...', self.thread._connection)
        self.thread._connection.ioloop.stop()
        # Note: All events still waiting on the ioloop are lost.
        # Messages are kept track of in the Queue.Queue or in the confirmer
        # module. Closing events are kept track on in shutter module.

        # Now we trigger the actual reconnection, which
        # works just like the first connection to RabbitMQ.
        self.builder.first_connection()




    '''
    This is called during reconnection and during channel reopen.
    Both implies that a new channel is opened.
    '''
    def __prepare_channel_reopen(self, operation_string):
        # We need to reset the message number, as
        # it works by channel:
        logdebug(LOGGER, operation_string+': Resetting delivery number (for publishing messages).')
        self.thread.reset_delivery_number()

        # Furthermore, as we'd like to re-publish messages
        # that had not been confirmed yet, we remove them
        # from the stack of unconfirmed messages, and put them
        # back to the stack of unpublished messages.
        logdebug(LOGGER, operation_string+': Sending all messages that have not been confirmed yet...')
        self.__prepare_republication_of_unconfirmed()

        # Reset the unconfirmed delivery tags, as they also work by channel:
        logdebug(LOGGER, operation_string+': Resetting delivery tags (for confirming messages).')
        self.thread.reset_unconfirmed_messages_and_delivery_tags()
        
    def __prepare_republication_of_unconfirmed(self):
        # Get all unconfirmed messages - we won't be able to receive their confirms anymore:
        # IMPORTANT: This has to happen before we reset the delivery_tags of the confirmer
        # module, as this deletes the collection of unconfirmed messages.
        rescued_messages = self.thread.get_unconfirmed_messages_as_list_copy_during_lifetime()
        if len(rescued_messages)>0:
            logdebug(LOGGER, '%s unconfirmed messages were saved and are sent now.', len(rescued_messages))
            self.thread.send_many_messages(rescued_messages)
            # Note: The actual publish of these messages to rabbit
            # happens when the connection is there again, so no wrong delivery
            # tags etc. are created by this line!


    ###############################
    ### React to channel close  ###
    ###############################


    '''
    Callback, called by RabbitMQ.
    "on_channel_closed" can be called in three situations:

    (1) The user asked to close the connection.
        In this case, we want to clean up everything and leave it closed.

    (2) The connection was closed because we tried to publish to a non-
        existent exchange.
        In this case, the connection is still open, and we want to reopen
        a new channel and publish to a different exchange.
        We also want to republish the ones that had failed.

    (3) There was some problem that closed the connection, which causes
        the channel to close.
        In this case, we want to reopen a connection.

    '''
    def on_channel_closed(self, channel, reply_code, reply_text):
        logdebug(LOGGER, 'Channel was closed: %s (code %s)', reply_text, reply_code)

        # Channel closed because user wants to close:
        if self.statemachine.is_PERMANENTLY_UNAVAILABLE():
            if self.statemachine.get_detail_closed_by_publisher():
                logdebug(LOGGER,'Channel close event due to close command by user. This is expected.')
            else:
                pass
                # TODO What to do here?

        # Channel closed because even fallback exchange did not exist:
        elif reply_code == 404 and "NOT_FOUND - no exchange 'FALLBACK'" in reply_text:
            self.__channel_was_closed_no_fallback()

        # Channel closed because exchange did not exist:
        elif reply_code == 404:
            self.__use_different_exchange_and_reopen_channel()

        # Other unexpected channel close:
        else:
            self.__channel_was_closed_unexpectedly()


    '''
    Define the behaviour if there was no FALLBACK exchange:
    We just try on with the next host.
    '''
    def __channel_was_closed_no_fallback(self):
        logerror(LOGGER,'Channel closed because FALLBACK exchange does not exist. Need to close connection to trigger all the necessary close down steps.')
        self.shutter.close_with_reconnection()

    '''
    Define the behaviour in case of a completely
    unexpected server-side channel close-down.
    As we don't know what goes on, let's just try to reconnect
    somewhere else.
    '''
    def __channel_was_closed_unexpectedly(self):
        logerror(LOGGER,'Unexpected channel shutdown. Need to close connection to trigger all the necessary close down steps.')
        self.shutter.close_with_reconnection()

    '''
    An attempt to publish to a nonexistent exchange will close
    the channel. In this case, we use a different exchange name
    and reopen the channel. The underlying connection was kept
    open.
    '''
    def __use_different_exchange_and_reopen_channel(self):
        logdebug(LOGGER, 'Channel closed because the exchange "%s" did not exist.', self.__nodemanager.get_exchange_name())

        # Set to waiting to be available, so that incoming
        # messages are stored:
        self.statemachine.set_to_waiting_to_be_available()

        # New exchange name
        logdebug(LOGGER, 'Setting exchange name to fallback exchange "%s"', defaults.RABBIT_FALLBACK_EXCHANGE_NAME)
        self.builder.set_exchange_name(defaults.RABBIT_FALLBACK_EXCHANGE_NAME)

        # If this happened while sending message to the wrong exchange, we
        # have to trigger their resending...
        self.__prepare_channel_reopen('Channel reopen')

        # Reopen channel
        # TODO Reihenfolge richtigen? Erst prepare, dann open?
        logdebug(LOGGER, 'Reopening channel...')
        self.statemachine.set_to_waiting_to_be_available()
        self.builder.please_open_rabbit_channel()
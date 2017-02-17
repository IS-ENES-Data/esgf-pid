import logging
import pika
import time
import copy
from esgfpid.utils import get_now_utc_as_formatted_string as get_now_utc_as_formatted_string
import esgfpid.defaults as defaults
import esgfpid.rabbit.connparams
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from esgfpid.rabbit.asynchronous.connection_doctor import ConnectionDoctor

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''

If the module fails connecting to a RabbitMQ node (on_connection_error),
or if the connection if interrupted (on_connection_closed),
it immediately tries connecting to the next RabbitMQ node.

If all hosts have been tried, the module starts over again, but waits some
seconds before that.

There is a maximum number of times that this is tried before
giving up Permanently.

'''
class ConnectionBuilder(object):
    
    def __init__(self, thread, statemachine, confirmer, returnhandler, shutter, nodemanager):
        self.__thread = thread
        self.__statemachine = statemachine

        '''
        We need to pass the "confirmer.on_delivery_confirmation()" callback to
        RabbitMQ's channel.'''
        self.__confirmer = confirmer
        
        '''
        We need to pass the "returnhandler.on_message_not_accepted()"" callback
        to RabbitMQ's channel as "on_return_callback" '''
        self.__returnhandler = returnhandler

        '''
        We need this to be able to trigger all the closing mechanisms 
        in case the module should close down as soon it was opened, i.e.
        if the close-command was issued while the connection was still
        building up.
        '''
        self.__shutter = shutter

        '''
        The node manager keeps all the info about the RabbitMQ nodes,
        e.g. URLs, usernames, passwords.
        '''
        self.__node_manager = nodemanager

        '''
        If the messages should not be published to the exchange that
        was passed from the publisher in config, but to a fallback 
        solution, this will be set:
        '''
        self.__fallback_exchange = None

        self.__connection_doctor = ConnectionDoctor(
            self,
            self.__thread,
            self.__statemachine,
            self.__shutter,
            self.__node_manager
        )

    # TODO CALLED BY WHO???
    def get_exchange_name(self):
        if self.__fallback_exchange is not None:
            return self.__fallback_exchange
        else:
            return self.__nodemanager.get_exchange_name()

    def set_exchange_name(self, new_name):
        self.__fallback_exchange = new_name


    ####################
    ### Start ioloop ###
    ####################

    '''
    Entry point. Called once to trigger the whole
    (re) connection process. Called from run method of the rabbit thread.
    '''
    def first_connection(self):
        logdebug(LOGGER, 'Trigger connection to rabbit...')
        self.__trigger_connection_to_rabbit_etc()
        logdebug(LOGGER, 'Trigger connection to rabbit... done.')
        logdebug(LOGGER, 'Start waiting for events...')
        self.__start_waiting_for_events()
        logtrace(LOGGER, 'Had started waiting for events, but stopped.')
    
    def __start_waiting_for_events(self):
        '''
        This waits until the whole chain of callback methods triggered by
        "trigger_connection_to_rabbit_etc()" has finished, and then starts 
        waiting for publications.
        This is done by starting the ioloop.

        Note: In the pika usage example, these things are both called inside the run()
        method, so I wonder if this check-and-wait here is necessary. Maybe not.
        But the usage example does not implement a Thread, so it probably blocks during
        the opening of the connection. Here, as it is a different thread, the run()
        might get called before the __init__ has finished? I'd rather stay on the
        safe side, as my experience of threading in Python is limited.
        '''

        # Start ioloop if connection object ready:
        if self.__thread._connection is not None:
            try:
                logdebug(LOGGER, 'Starting ioloop...')
                logtrace(LOGGER, 'ioloop is owned by connection %s...', self.__thread._connection)

                # Tell the main thread that we're now open for events.
                # As soon as the thread._connection object is not None anymore, it
                # can receive events.
                self.__thread.tell_publisher_to_stop_waiting_for_thread_to_accept_events() 
                self.__thread.continue_gently_closing_if_applicable()
                self.__thread._connection.ioloop.start()

            except pika.exceptions.ProbableAuthenticationError as e:

                # If the library was stopped by the user in the mean time,
                # we do not try to reconnect.
                if self.__statemachine.is_PERMANENTLY_UNAVAILABLE():
                    logerror(LOGGER, 'Caught Authentication Exception during connection ("%s"). Will not reconnect.', e.__class__.__name__)
                    # We do not do anything anymore, as the shutdown has been
                    # handled already, otherwise, the state would not be
                    # permanently unavailable.
                
                # In normal cases, we do try to reconnect. As we will try
                # to reconnect, we set state to waiting to connect.
                else:
                    logerror(LOGGER, 'Caught Authentication Exception during connection ("%s"). Will try to reconnect to next host.', e.__class__.__name__)
                    self.__statemachine.set_to_waiting_to_be_available()
                    self.__statemachine.detail_authentication_exception = True # TODO WHAT FOR?

                    # It seems that ProbableAuthenticationErrors do not cause
                    # RabbitMQ to call any callback, either on_connection_closed
                    # or on_connection_error - it just silently swallows the
                    # problem.
                    # So we need to manually trigger reconnection to the next
                    # host here, which we do by manually calling the callback.
                    errorname = 'ProbableAuthenticationError issued by pika'
                    self.__connection_doctor.on_connection_error(self.__thread._connection, errorname)

                    # We start the ioloop, so it can handle the reconnection events,
                    # or also receive events from the publisher in the meantime.
                    self.__thread._connection.ioloop.start()

            except Exception as e:
                # This catches any error during connection startup and during the entire
                # time the ioloop runs, blocks and waits for events.
                logerror(LOGGER, 'Unexpected error during event listener\'s lifetime: %s: %s', e.__class__.__name__, e.message)

                # As we will try to reconnect, set state to waiting to connect.
                # If reconnection fails, it will be set to permanently unavailable.
                self.__statemachine.set_to_waiting_to_be_available()

                # In case this error is reached, it seems that no callback
                # was called that handles the problem. Let's try to reconnect
                # somewhere else.
                errorname = 'Unexpected error ('+str(e.__class__.__name__)+': '+str(e.message)+')'
                ###TODOself.__connection_doctor.on_connection_error(self.__thread._connection, errorname)
                raise e

                # We start the ioloop, so it can handle the reconnection events,
                # or also receive events from the publisher in the meantime.
                self.__thread._connection.ioloop.start()
        
        else:
            # I'm quite sure that this cannot happen, as the connection object
            # is created in "trigger_connection_...()" and thus exists, no matter
            # if the actual connection to RabbitMQ succeeded (yet) or not.
            logdebug(LOGGER, 'This cannot happen: Connection object is not ready.')
            logerror(LOGGER, 'Cannot happen. Cannot properly start the thread. Connection object is not ready.')

    ########################################
    ### Chain of callback functions that ###
    ### connect to rabbit                ###
    ########################################

    def __trigger_connection_to_rabbit_etc(self):
        self.__statemachine.set_to_waiting_to_be_available()
        self.__please_open_connection()

    ''' Asynchronous, waits for answer from RabbitMQ.'''
    def __please_open_connection(self):
        params = self.__node_manager.get_connection_parameters()
        logdebug(LOGGER, 'Connecting to RabbitMQ at %s... (%s)',
            params.host, get_now_utc_as_formatted_string())
        loginfo(LOGGER, 'Opening connection to RabbitMQ...')
        self.__thread._connection = pika.SelectConnection(
            parameters=params,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.__connection_doctor.on_connection_error,
            on_close_callback=self.on_connection_closed,
            stop_ioloop_on_close=False # TODO Why not?
        )

    ''' Callback, called by RabbitMQ.'''
    def on_connection_open(self, unused_connection):
        logdebug(LOGGER, 'Opening connection... done.')
        loginfo(LOGGER, 'Connection to RabbitMQ at %s opened... (%s)',
            self.__node_manager.get_connection_parameters().host,
            get_now_utc_as_formatted_string())

        # Tell the main thread we're open for events now:
        # When the connection is open, the thread is ready to accept events.
        # Note: It was already ready when the connection object was created,
        # not just now that it's actually open. There was already a call to
        # "...stop_waiting..." in start_waiting_for_events(), which quite
        # certainly was carried out before this callback. So this call to
        # "...stop_waiting..." is likelily redundant!
        self.__thread.tell_publisher_to_stop_waiting_for_thread_to_accept_events()
        self.please_open_rabbit_channel()

    '''
    Asynchronous, waits for answer from RabbitMQ.
    This needs to be public, so the ConnectionDoctor
    can call it, when it needs to reopen a channel
    to remedy some channel problem.
    '''
    def please_open_rabbit_channel(self):
        logdebug(LOGGER, 'Opening channel...')
        self.__thread._connection.channel(on_open_callback=self.on_channel_open)

    ''' Callback, called by RabbitMQ. '''
    def on_channel_open(self, channel):
        logdebug(LOGGER, 'Opening channel... done.')
        logtrace(LOGGER, 'Channel has number: %s.', channel.channel_number)

        # Store the channel object TODO for what?
        self.__thread._channel = channel

        # When we have successfully (re)connected, we
        # reset the counter of reconnection attempts:
        self.__connection_doctor.reset_reconnection_counter()

        # Adding the necessary callbacks to the channel:
        self.__add_on_channel_close_callback()
        self.__add_on_return_callback()
        self.__make_channel_confirm_delivery()

        # Check whether anything relevant has happened
        # in the meantime, set the correct state,
        # publish messages if any have arrived in the
        # mean time...
        self.__make_ready_for_publishing()

    def __make_channel_confirm_delivery(self):
        logtrace(LOGGER, 'Set confirm delivery... (Issue Confirm.Select RPC command)')
        self.__thread._channel.confirm_delivery(callback=self.__confirmer.on_delivery_confirmation)
        logdebug(LOGGER, 'Set confirm delivery... done.')
 
    def __make_ready_for_publishing(self):
        logdebug(LOGGER, '(Re)connection established, making ready for publication...')

        # Check for unexpected errors:
        if self.__thread._channel is None:
            logerror(LOGGER, 'Channel is None after connecting to server. This should not happen.')
            self.__statemachine.set_to_permanently_unavailable()
        if self.__thread._connection is None:
            logerror(LOGGER, 'Connection is None after connecting to server. This should not happen.')
            self.__statemachine.set_to_permanently_unavailable()

        # Normally, it should already be waiting to be available:
        if self.__statemachine.is_WAITING_TO_BE_AVAILABLE():
            logdebug(LOGGER, 'Setup is finished. Publishing may start.')
            logtrace(LOGGER, 'Publishing will use channel no. %s!', self.__thread._channel.channel_number)
            self.__statemachine.set_to_available()
            self.__check_for_already_arrived_messages_and_publish_them()

        # It was asked to close in the meantime (but might be able to publish the last messages):
        elif self.__statemachine.is_AVAILABLE_BUT_WANTS_TO_STOP():
            logdebug(LOGGER, 'Setup is finished, but the module was already asked to be closed in the meantime.')
            self.__check_for_already_arrived_messages_and_publish_them()

        # It was force-closed in the meantime:
        elif self.__statemachine.is_PERMANENTLY_UNAVAILABLE(): # state was set in shutter module's __close_down()
            if self.__statemachine.get_detail_closed_by_publisher():
                logdebug(LOGGER, 'Setup is finished now, but the module was already force-closed in the meantime.')
                self.__shutter.safety_finish('closed before connection was ready. reclosing.') # TODO Why?
            elif self.__statemachine.detail_could_not_connect:
                logerror(LOGGER, 'This is not supposed to happen. If the connection failed, this part of the code should not be reached.')
            else:
                logerror(LOGGER, 'This is not supposed to happen. An unknown event set this module to be unavailable. When was this set to unavailable?')
        else:
            logdebug(LOGGER, 'Unexpected state.')

    def __check_for_already_arrived_messages_and_publish_them(self):
        logdebug(LOGGER, 'Checking if messages have arrived in the meantime...')
        num = self.__thread.get_num_unpublished()
        if num > 0:
            loginfo(LOGGER, 'Ready to publish messages to RabbitMQ. %s messages are already waiting to be published.', num)
            for i in xrange(num):
                self.__thread.add_event_publish_message()
        else:
            loginfo(LOGGER, 'Ready to publish messages to RabbitMQ.')
            logdebug(LOGGER, 'Ready to publish messages to RabbitMQ. No messages waiting yet.')
        

   
    #############################
    ### React to channel and  ###
    ### connection close      ###
    #############################

    '''
    This tells RabbitMQ what to do if it receives 
    a message it cannot accept, e.g. if it cannot
    route it.
    '''
    def __add_on_return_callback(self):
        self.__thread._channel.add_on_return_callback(
            self.__returnhandler.on_message_not_accepted
        )

    '''
    This tells RabbitMQ what to do if the channel
    was closed. Channel shutdowns are handles by
    the connection_doctor, as they are usually
    errors.

    Note:
    Every connection close includes a channel close.
    However, as far as I know, this callback is only
    called if the channel is closed without the underlying
    connection being closed. I am not 100 percent sure though.
    '''
    def __add_on_channel_close_callback(self):
        self.__thread._channel.add_on_close_callback(
            self.__connection_doctor.on_channel_closed
        )



  
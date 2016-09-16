import logging
import pika
import time
import copy
import esgfpid.defaults as defaults
import esgfpid.rabbit.connparams
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())
LOGGER_IOLOOP = LOGGER.getChild('ioloop')

class ConnectionBuilder(object):
    
    def __init__(self, thread, statemachine, confirmer, feeder, acceptor, shutter, args):
        self.thread = thread
        self.statemachine = statemachine
        self.confirmer = confirmer
        self.feeder = feeder
        self.acceptor = acceptor
        self.shutter = shutter

        self.__store_settings_for_rabbit(args)
        self.__inform_about_settings()
        self.__reconnect_counter = 0
        self.__reconnect_max_tries = 5
        #self.__reconnect_wait_seconds = 3

    def __store_settings_for_rabbit(self, args):
        self.args = args
        self.RABBIT_HOSTS = copy.copy(args['urls_fallback'])
        self.CURRENT_HOST = copy.copy(args['url_preferred'])
        self.CREDENTIALS = copy.copy(args['credentials'])
        self.EXCHANGE = copy.copy(args['exchange_name']) # only for logging it.

    def __inform_about_settings(self):
        logdebug(LOGGER, 'init:Messaging server exchange: "%s".', self.EXCHANGE)
        logdebug(LOGGER, 'init:Messaging server URL: "%s".', self.CURRENT_HOST)
        logdebug(LOGGER, 'init:Messaging server username: "%s".', self.CREDENTIALS.username)
        logdebug(LOGGER, 'init:Messaging server password: "%s".', self.CREDENTIALS.password)
        if len(self.RABBIT_HOSTS) > 0:
            for i in xrange(len(self.RABBIT_HOSTS)):
                logdebug(LOGGER, 'init:Fallback URL %i: "%s".', i+1, self.RABBIT_HOSTS[i])
        else:
            logdebug(LOGGER, 'init:No fallback URLs provided.')

    ####################
    ### Start ioloop ###
    ####################
    
    def start_ioloop_when_connection_ready(self, max_retries=10, retry_seconds=0.5): # TODO Put these values into config!
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
        counter_of_tries = 0
        while True:

            # Make sure we can not loop eternally:
            counter_of_tries += 1
            if counter_of_tries > max_retries:
                msg = 'Cannot properly start the thread. The connection to rabbit takes too much time.'
                logwarn(LOGGER_IOLOOP, msg)
                raise ValueError(msg)

            # If connection returned an error, do not try any longer:
            if self.statemachine.could_not_connect:
                msg = 'Cannot properly start the thread. The connection to rabbit failed.'
                logwarn(LOGGER_IOLOOP, msg)
                #raise ValueError(msg) # TODO
                break

            # Start ioloop if ready:
            started = self.__start_ioloop_if_connection_ready()
            if started: # This code is reached after ioloop was closed!
                logtrace(LOGGER_IOLOOP, 'Had started ioloop in %ith try.', counter_of_tries)
                break

            # Otherwise, wait (for retry)
            else:
                self.__log_connection_not_ready(counter_of_tries, max_retries)
                time.sleep(retry_seconds)

    def __start_ioloop_if_connection_ready(self):
        ready = self.__connection_is_ready()
        if ready:
            try:
                logdebug(LOGGER_IOLOOP, 'Connection is ready. Ioloop about to be started.', show=True)
                self.thread._connection.ioloop.start()
                return True
            except pika.exceptions.ProbableAuthenticationError as e: # TODO
                LOGGER_IOLOOP.exception('Error when creating ioloop: '+e.message)
                logerror(LOGGER_IOLOOP, 'Caught Authentication Exception: '+e.message)
                return True # If we return False, it will reconnect!
            except Exception as e: # TODO
                logerror(LOGGER_IOLOOP, 'Unexpected error: '+str(e.message))
                return True # If we return False, it will reconnect!
        else:
            logtrace(LOGGER_IOLOOP, 'Connection not ready. Cannot start ioloop yet.')
            return False

    def __connection_is_ready(self):
        logtrace(LOGGER_IOLOOP, 'Connection: %s', self.thread._connection)
        if self.thread._connection is None:
            logtrace(LOGGER_IOLOOP, 'Connection is None, cannot start ioloop yet.', show=True)
            return False
        else:
            return True # Connection does not need to be open yet, just exist.

    def __log_connection_not_ready(self, counter_of_tries, max_retries):
        if counter_of_tries == max_retries:
            logtrace(LOGGER_IOLOOP, 'Connection is not ready in try %i/%i. Giving up.', counter_of_tries, max_retries)
        else:
            logtrace(LOGGER_IOLOOP, 'Connection is not ready in try %i/%i. Trying again.', counter_of_tries, max_retries)
    

    ########################################
    ### Chain of callback functions that ###
    ### connect to rabbit                ###
    ########################################

    def trigger_connection_to_rabbit_etc(self):
        # Called once to trigger the whole (re)connection process.
        # This changes the state of the state machine.
        logdebug(LOGGER, 'Start connecting to message queueing server...', show=True)
        self.statemachine.set_to_waiting_to_be_available()
        self.__connect_to_rabbit()

    def __connect_to_rabbit(self):
        # May be called several times in case connection fails (once for each host).
        params = self.__get_connection_params()
        self.thread._connection = self.__please_open_connection(params)

    def __get_connection_params(self):
        logdebug(LOGGER, 'Connecting to %s.', self.CURRENT_HOST)
        return esgfpid.rabbit.connparams.get_connection_parameters(
            self.CREDENTIALS,
            self.CURRENT_HOST)
        
    def __please_open_connection(self, params):
        # Asynchronous: Process is continued on "on_connection_open"
        logdebug(LOGGER, 'Opening connection to "%s"...', params.host, show=True)
        #return pika.SelectConnection(
        #    parameters=params,
        #    on_open_callback=self.on_connection_open,
        #    on_open_error_callback=self.on_connection_error,
        #    on_close_callback=None,
        #    stop_ioloop_on_close=False
        #)
        return pika.SelectConnection(
            params,
            self.on_connection_open,
            self.on_connection_error,
            stop_ioloop_on_close=False
        )

    def on_connection_open(self, unused_connection):
        logdebug(LOGGER, 'Opening connection... done.', show=True)
        self.__add_on_connection_close_callback()
        self.__please_open_rabbit_channel()

    def __please_open_rabbit_channel(self):
        # Asynchronous: Process is continued on "on_channel_open"
        logdebug(LOGGER, 'Opening channel...', show=True)
        self.thread._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        logdebug(LOGGER, 'Opening channel... done.', show=True)
        self.thread._channel = channel
        self.__reconnect_counter = 0
        self.__add_on_channel_close_callback()
        self.__add_on_return_callback()
        self.__make_channel_confirm_delivery()
        self.__make_ready_for_publishing()

    def __make_channel_confirm_delivery(self):
        logtrace(LOGGER, 'Set confirm delivery... (Issue Confirm.Select RPC command)')
        self.thread._channel.confirm_delivery(callback=self.confirmer.on_delivery_confirmation)
 
    def __make_ready_for_publishing(self):
        # This changes the state of the state machine!
        logdebug(LOGGER, 'Start connecting to message queueing server... done', show=True)

        # Check for unexpected errors:
        if self.thread._channel is None:
            logerror(LOGGER, 'Channel is None after connecting to server. This should not happen.')
            self.statemachine.set_to_permanently_unavailable()
        if self.thread._connection is None:
            logerror(LOGGER, 'Connection is None after connecting to server. This should not happen.')
            self.statemachine.set_to_permanently_unavailable()

        # Normally, it should already be waiting to be available:
        if self.statemachine.is_waiting_to_be_available():
            logdebug(LOGGER, 'Setup is finished. Publishing may start!', show=True)
            self.statemachine.set_to_available()
            self.__check_for_already_arrived_messages()

        # It was asked to close in the meantime (but might be able to publish the last messages):
        elif self.statemachine.is_available_but_wants_to_stop():
            logdebug(LOGGER, 'Setup is finished, but the module was already asked to be closed in the meantime.')
            #self.statemachine.set_to_wanting_to_stop()
            self.__check_for_already_arrived_messages()

        # It was force-closed in the meantime:
        elif self.statemachine.is_permanently_unavailable():
            if self.statemachine.closed_by_publisher:
                logdebug(LOGGER, 'Setup is finished now, but the module was already force-closed in the meantime.')
                self.__reclose_freshly_made_connection()
            elif self.statemachine.could_not_connect:
                logerror(LOGGER, 'This is not supposed to happen. If the connection failed, this part of the code should not be reached.')
            else:
                logerror(LOGGER, 'This is not supposed to happen. An unknown event set this module to be unavailable. When was this set to unavailable?')    

    def __check_for_already_arrived_messages(self):
        logdebug(LOGGER, 'Checking if messages have arrived in the meantime...', show=True)
        num = self.feeder.get_num_unpublished()
        if num > 0:
            logdebug(LOGGER, 'Yes, there is %i messages!', num, show=True)
            self.acceptor.trigger_publishing_n_messages_if_ok(num)
        else:
            logdebug(LOGGER, 'No, no messages are waiting to be published.', show=True)

    def __reclose_freshly_made_connection(self):
        self.shutter.safety_finish('closed before connection was ready. reclosing.')

    ########################
    ### Connection error ###
    ########################

    def on_connection_error(self, connection, msg):
        if self.__is_fallback_url_left():
            logdebug(LOGGER, 'Failed connection to "%s": %s. %i fallback URLs left to try.',
                self.CURRENT_HOST, msg, len(self.RABBIT_HOSTS), show=True)
            self.__try_connecting_to_next(msg)
        else:
            self.__reconnect_counter += 1;
            if self.__reconnect_counter <= self.__reconnect_max_tries:
                # wait and reconnect:
                self.__reset_hosts()
                #time.sleep(self.__reconnect_wait_seconds)
                #self.__connect_to_rabbit()
                self.__make_reconnect_soon(connection, 0, msg)
            else:
                self.__change_state_to_permanently_could_not_connect()
                self.__inform_permanently_could_not_connect(msg)
        return None

    def __is_fallback_url_left(self):
        num_fallbacks = len(self.RABBIT_HOSTS)
        if num_fallbacks > 0:
            return True
        else:
            return False

    def __reset_hosts(self):
        self.__store_settings_for_rabbit(self.args)
 
    def __change_state_to_permanently_could_not_connect(self):
        # This changes the state of the state machine.
        self.statemachine.set_to_permanently_unavailable()
        self.statemachine.could_not_connect = True # TODO THIS SHOULD ONLY BE USED AS INFO
 
    def __inform_permanently_could_not_connect(self, msg):
        logdebug(LOGGER, 'Failed connection to "%s": %s. No fallback URL left to try.',
            self.CURRENT_HOST, msg, show=True)
        logwarn(LOGGER, 'Failed to connect to messaging service.')

    def __try_connecting_to_next(self, msg):
        nexthost = self.RABBIT_HOSTS.pop()
        self.CURRENT_HOST = nexthost
        self.__connect_to_rabbit()

    #############################
    ### React to channel and  ###
    ### connection close      ###
    #############################

    def __add_on_return_callback(self):
        self.thread._channel.add_on_return_callback(self.acceptor.on_message_not_accepted)

    def __add_on_connection_close_callback(self):
        self.thread._connection.add_on_close_callback(self.on_connection_closed)

    def __add_on_channel_close_callback(self):
        self.thread._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        logdebug(LOGGER, 'Channel was closed: %s (code %i)', reply_text, reply_code, show=True)
        if self.statemachine.is_permanently_unavailable():
            if self.statemachine.closed_by_publisher:
            #if self.__was_user_shutdown(reply_code, reply_text):
                logtrace(LOGGER,'Channel close event due to close command by user. This is expected.')
        else:
            logtrace(LOGGER,'Unexpected channel shutdown. Need to close connection to trigger all the necessary close down steps.')
            self.thread._connection.close()

    def on_connection_closed(self, connection, reply_code, reply_text):
        loginfo(LOGGER, 'Connection was closed: %s (code %i)', reply_text, reply_code, show=True)
        self.thread._channel = None
        if self.__was_user_shutdown(reply_code, reply_text):
            self.__make_permanently_closed_by_user()
        else:
            self.__make_reconnect_soon(connection, reply_code, reply_text)

    def __was_user_shutdown(self, reply_code, reply_text):
        if self.__was_forced_user_shutdown(reply_code, reply_text):
            return True
        elif self.__was_gentle_user_shutdown(reply_code, reply_text):
            return True
        return False

    def __was_forced_user_shutdown(self, reply_code, reply_text):
        if (reply_code==self.thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER and
            self.thread.ERROR_TEXT_CONNECTION_FORCE_CLOSED in reply_text):
            return True
        return False

    def __was_gentle_user_shutdown(self, reply_code, reply_text):
        if (reply_code==self.thread.ERROR_CODE_CONNECTION_CLOSED_BY_USER and
            self.thread.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN in reply_text):
            return True
        return False

    def __make_permanently_closed_by_user(self):
        # This changes the state of the state machine!
        self.statemachine.set_to_permanently_unavailable()
        logtrace(LOGGER, 'Stopping ioloop due to user interrupt!')
        self.thread._connection.ioloop.stop()
        logdebug(LOGGER, 'Connection to messaging service closed by user. Will not reopen.')

    def __make_reconnect_soon(self, connection, reply_code, reply_text):
        # This changes the state of the state machine!
        self.statemachine.set_to_waiting_to_be_available()
        reopen_seconds = defaults.RABBIT_ASYN_RECONNECTION_SECONDS
        logwarn(LOGGER, 'Connection to messaging service closed, reopening in %i seconds. %s (code %i)',
            reopen_seconds, reply_text, reply_code)
        connection.add_timeout(reopen_seconds, self.reconnect)
        logtrace(LOGGER, 'Reconnect request added to connection %s!', connection)
        logtrace(LOGGER, 'Not to connection %s!', self.thread._connection)

    ###########################
    ### Reconnect after     ###
    ### unexpected shutdown ###
    ###########################

    def reconnect(self):

        # Reset the message number, as it works by connections:
        self.feeder.reset_message_number()

        # Get all unconfirmed messages - we won't be able to receive their confirms anymore:
        # IMPORTANT: This has to happen before we reset the delivery_tags of the confirmer
        # module, as this deletes the collection of unconfirmed messages.
        rescued_messages = self.confirmer.get_unconfirmed_messages_as_list_copy()
        if len(rescued_messages)>0:
            self.acceptor.send_many_messages(rescued_messages)
            # Note: The actual publish of these messages to rabbit
            # happens when the connection is there again, so no wrong delivery
            # tags etc. are created by this line!

        # Reset the unconfirmed delivery tags, as they also work by connections:
        self.confirmer.reset_delivery_tags()

        # This is the old connection IOLoop instance, stop its ioloop
        self.thread._connection.ioloop.stop()

        # Create a new connection
        self.trigger_connection_to_rabbit_etc()

        # There is now a new connection, needs a new ioloop to run
        self.__start_ioloop_if_connection_ready()


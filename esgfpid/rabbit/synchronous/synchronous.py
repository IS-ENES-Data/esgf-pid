import logging
import datetime
import json
import pika
import time
import esgfpid.utils
import esgfpid.defaults as defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn
from esgfpid.exceptions import MessageNotDeliveredException
from esgfpid.utils import get_now_utc_as_formatted_string as get_now_utc_as_formatted_string
from .. import rabbitutils as rabbitutils
from ..exceptions import PIDServerException


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class SynchronousRabbitConnector(object):

    '''
    Constructor for the sychronous rabbit connection module.

    This does not open a connection yet.

    :param node_manager: NodeManager object that contains 
        the info about all the available RabbitMQ instances,
        their credentials, their priorities.

    '''
    def __init__(self, nodemanager):

        loginfo(LOGGER, 'Init of SynchronousRabbitConnector!!! Bla')

        '''
        NodeManager provices info about all
        hosts.
        '''
        self.__nodemanager = nodemanager

        '''
        Props for basic_publish. Does not
        depend on host, so store it once for all.
        '''
        self.__props = self.__nodemanager.get_properties_for_message_publications()

        '''
        To count how many times we have tried to reconnect the set of
        RabbitMQ hosts.
        '''
        self.__reconnect_counter = 0

        '''
        To see how many times we should try reconnecting to the set 
        of RabbitMQ hosts. Note that if there is 3 hosts, and we try 2
        times, this means 6 connection tries in total.
        '''
        self.__max_reconnection_tries = defaults.RABBIT_RECONNECTION_MAX_TRIES

        '''
        How many seconds to wait before reconnecting after having tried
        all hosts. (There is no waiting time trying to connect to a different
        host after one fails).
        '''
        self.__wait_seconds_before_reconnect = defaults.RABBIT_RECONNECTION_SECONDS

        '''
        To see how much time it takes to connect. Once a connection is
        established or failed, we print the time delta to logs.
        '''
        self.__start_connect_time = None

        '''
        If the messages should not be published to the exchange that
        was passed from the publisher in config, but to a fallback 
        solution, this will be set:
        '''
        self.__fallback_exchange = None

        ''' Set of all tried hosts, for logging. '''
        self.__all_hosts_that_were_tried = set()



        # Defaults:
        self.__mandatory_flag = esgfpid.defaults.RABBIT_MANDATORY_DELIVERY
        self.__max_tries = esgfpid.defaults.RABBIT_SYN_MESSAGE_MAX_TRIES
        self.__timeout_milliseconds = esgfpid.defaults.RABBIT_SYN_MESSAGE_TIMEOUT_MILLISEC
        self.__candidate_fallback_exchange_name = defaults.RABBIT_FALLBACK_EXCHANGE_NAME

        # Other settings:
        self.__channel = None
        self.__connection = None
        self.__communication_established = False
        self.__connection_last_process_event_call = 0
        self.__error_messages_during_init = []


    ########################################
    ### This actually connects to rabbit ###
    ########################################

    '''
    Opens a connection and channel to RabbitMQ.

    It tries connecting to all available hosts until
    it succeeds, or until the maximum number of
    attempts per host is reached.

    The sequence of the hosts is determined
    by the NodeManager.

    The maximum number of connection attempts is
    determined in the esgfpid.defaults.

    '''
    def open_rabbit_connection(self):    

        continue_connecting = True
        while continue_connecting:

            success = self.__try_connecting_to_next()

            if success:
                continue_connecting = False

            else:

                # Log failure:
                oldhost = self.__nodemanager.get_connection_parameters().host
                time_passed = datetime.datetime.now() - self.__start_connect_time
                loginfo(LOGGER, 'Failed connection to RabbitMQ at %s after %s seconds.', oldhost, time_passed.total_seconds())

                # If there is alternative URLs, try one of them:
                if self.__nodemanager.has_more_urls():
                    logdebug(LOGGER, 'Connection failure: %s fallback URLs left to try.', self.__nodemanager.get_num_left_urls())
                    self.__nodemanager.set_next_host()
                    newhost = self.__nodemanager.get_connection_parameters().host
                    loginfo(LOGGER, 'Next connection attempt (now) %s.', newhost)

                # If there is no URLs, reset the node manager to
                # start at the first nodes again...
                else:
                    self.__reconnect_counter += 1;
                    if self.__reconnect_counter <= self.__max_reconnection_tries:
                        reopen_seconds = self.__wait_seconds_before_reconnect
                        logdebug(LOGGER, 'Connection failure: Failed connecting to all hosts. Waiting %s seconds and starting over.', reopen_seconds)
                        self.__nodemanager.reset_nodes()
                        newhost = self.__nodemanager.get_connection_parameters().host
                        loginfo(LOGGER, 'Next connection attempt (in %s seconds) to %s.', reopen_seconds, newhost)
                        time.sleep(reopen_seconds)

                    # Give up after so many tries...
                    else:
                        continue_connecting = False
                        errormsg = ('Permanently failed to connect to RabbitMQ. Tried all hosts %s %s times. Giving up. No PID requests will be sent.' % (list(self.__all_hosts_that_were_tried) ,self.__max_reconnection_tries))
                        logerror(LOGGER, errormsg)
                        collected_errors = ' - '.join(self.__error_messages_during_init)
                        logwarn(LOGGER, 'No connection possible. Errors: %s' % collected_errors)
                        raise PIDServerException(errormsg)

    '''
    Opens and stores connection and channel to the next
    RabbitMQ host. The order of the hosts is determined
    by the NodeManager object.

    :returns: True if successful. False otherwise.
    '''
    def __try_connecting_to_next(self):
        success = False

        # Open connection
        params = self.__nodemanager.get_connection_parameters()
        connection = self.__setup_rabbit_connection(params)

        # If connection is ok, open channel
        if connection is not None and connection.is_open:
            self.__connection = connection
            LOGGER.debug('Succeeded opening connection to '+str(params.host))
            channel = self.__setup_rabbit_channel()

            # If channel ok, success, yeah!
            if channel is not None and channel.is_open:
                LOGGER.debug('Succeeded opening channel.')
                self.__channel = channel
                self.__channel.confirm_delivery()
                self.__communication_established = True
                success = True
            else:
                LOGGER.debug('Could not open channel.')

        return success

    '''
    Opens the Connection to RabbitMQ instance and returns it.

    :param params: Parameters for connection to RabbitMQ, of
        type pika.ConnectionParameters.
    :returns: The connection, if it was built successfully. Returns
        None if there was an expected error that should be dealt 
        with by trying the next host.
    :raises: Any unexpected exception.
    '''
    def __setup_rabbit_connection(self, params):
        LOGGER.debug('Setting up the connection with the RabbitMQ.')
        self.__start_connect_time = datetime.datetime.now()
        self.__all_hosts_that_were_tried.add(params.host)
        
        try:
            time_now = get_now_utc_as_formatted_string()
            logdebug(LOGGER, 'Connecting to RabbitMQ at %s... (%s)', params.host, time_now)

            # Make connection
            conn = self.__make_connection(params)

            # Log success
            time_now = get_now_utc_as_formatted_string()
            time_passed = datetime.datetime.now() - self.__start_connect_time
            time_seconds = time_passed.total_seconds()
            loginfo(LOGGER, 'Connection to RabbitMQ at %s opened after %i seconds... (%s)', params.host, time_seconds, time_now)
            return conn

        except pika.exceptions.ProbableAuthenticationError as e:
            time_passed = datetime.datetime.now() - self.__start_connect_time
            time_seconds = time_passed.total_seconds()
            error_name = e.__class__.__name__
            logerror(LOGGER, 'Caught Authentication Exception after %s seconds during connection ("%s").', time_seconds, error_name)
            msg = ('Problem setting up the rabbit with username "%s" and password "%s" at url %s.' %
                    (params.credentials.username,
                     params.credentials.password,
                     params.host))
            LOGGER.error(msg)
            self.__error_messages_during_init.append(msg)
            return None

        except pika.exceptions.AMQPConnectionError as e:
            time_passed = datetime.datetime.now() - self.__start_connect_time
            time_seconds = time_passed.total_seconds()
            error_name = e.__class__.__name__
            logerror(LOGGER, 'Caught AMQPConnectionError Exception after %s seconds during connection ("%s").', time_seconds, error_name)
            msg = ('Problem setting up the rabbit connection to %s.' % params.host)
            self.__error_messages_during_init.append(msg)
            return None

        except Exception as e:
            time_passed = datetime.datetime.now() - self.__start_connect_time
            time_seconds = time_passed.total_seconds()
            error_name = e.__class__.__name__
            logerror(LOGGER, 'Error ("%s") during connection to %s, after %s seconds.',
                error_name, params.host, time_seconds)
            msg = ('Unexpected problem setting up the rabbit connection to %s (%s)' % (params.host, error_name))
            self.__error_messages_during_init.append(msg)
            raise e

    '''
    Little wrapper around pika's connection opening
    method, so we can mock this during unit tests.
    '''
    def __make_connection(self, params): # this is easy to mock during unit tests        
        return pika.BlockingConnection(params)

    '''
    Open a communication channel to RabbitMQ in the
    connection previously opened and stored.

    :returns: A pika.Channel object. Returns None is there was
        an expected exception.
    '''
    def __setup_rabbit_channel(self):
        try:
            return self.__make_channel()
        except pika.exceptions.ChannelClosed:
            msg = ('Problem setting up the rabbit channel.')
            self.__error_messages_during_init.append(msg)
            LOGGER.warn(msg)
            return None

    '''
    Little wrapper around pika's channel opening
    method, so we can mock this during unit tests.
    '''
    def __make_channel(self): # this is easy to mock during unit tests
        return self.__connection.channel()

    ########################
    ### Close connection ###
    ########################

    def close_rabbit_connection(self):
        if (self.__connection is not None) and (self.__connection.is_open):
            self.__connection.close()
            # From pika doc: If there are any open channels, it will attempt to
            # close them prior to fully disconnecting

    ####################
    ### Send message ###
    ####################

    '''
    Send a message to RabbitMQ, synchronously. If the
    delivery was not successful, an exception is raised.

    :param: JSON message. TODO what needs to be included?
    :raises: esgfpid.exceptions.MessageNotDeliveredException:
        In case the message was not delivered.
    '''
    def send_message_to_queue(self, message):
        self.__open_connection_if_not_open()
        routing_key, msg_string = rabbitutils.get_routing_key_and_string_message_from_message_if_possible(message)
        success = False
        error_msg = None

        try:
            success = self.__try_sending_message_several_times(routing_key, msg_string)

        except pika.exceptions.UnroutableError:
            logerror(LOGGER, 'Refused message with routing key "%s".' % routing_key)
            body_json = json.loads(msg_string)
            body_json, new_routing_key = rabbitutils.add_emergency_routing_key(body_json)
            logerror(LOGGER, 'Refused message with routing key "%s". Resending with "%s".' % (routing_key, new_routing_key))
            routing_key = new_routing_key
            try:
                success = self.__try_sending_message_several_times(routing_key, msg_string)
            except pika.exceptions.UnroutableError:
                error_msg = 'The RabbitMQ node refused a message a second time'
                error_msg += ' with the original routing key "%s" and the emergency routing key "%s").' % (body_json['original_routing_key'], routing_key)
                error_msg += ' Dropping the message.'
                logerror(LOGGER, error_msg)

        if not success:
            raise MessageNotDeliveredException(error_msg, msg_string)

    def __open_connection_if_not_open(self):
        if not self.__communication_established:
            self.open_rabbit_connection()

    def __try_sending_message_several_times(self, routing_key, message):
        delivered_after_x_times = self.__retry_x_times(routing_key, message)
        if delivered_after_x_times:
            return True
        else:
            LOGGER.error('Message could not be sent: %s', message)
            return False

    def __retry_x_times(self, routing_key, messagebody):
        message_delivered = False
        max_tries_reached = False
        counter = 1
        while (not message_delivered) and (not max_tries_reached):

            # Try to deliver:
            #LOGGER.debug('Delivery no. '+str(counter)')
            delivered_now = self.__send_message_to_queue_once(routing_key, messagebody)

            # Check if it was delivered:
            if delivered_now:
                logdebug(LOGGER, 'Successful delivery of message with routing key "%s"!', routing_key)
                logdebug(LOGGER, 'Delivered after %i times!', counter)
                message_delivered = True
            else:
                logdebug(LOGGER, 'Not delivered, waiting %i milliseconds before delivering it again!', self.__timeout_milliseconds)
                time.sleep(self.__timeout_milliseconds/1000)

            # Increment counter, check if we have reached the max:
            if counter == self.__max_tries:
                max_tries_reached = True
                logdebug(LOGGER, 'Tried %i times, giving up!', self.__max_tries)
            counter += 1

        # Return with or without success:
        return message_delivered

    def __send_message_to_queue_once(self, routing_key, messagebody):
        delivered = False
        try:
            delivered = self.__do_send_message(routing_key, messagebody, self.__props)
            self.__avoid_connection_shutdown()
            
        except pika.exceptions.UnroutableError:
            logerror(LOGGER, 'Message could not be routed to any queue, maybe none was declared yet.')
            raise

        return delivered

    def __do_send_message(self, routing_key, messagebody, props): # This is easy to mock during unit tests
        return self.__channel.basic_publish(
            exchange=self.__get_exchange_name(),
            routing_key=routing_key,
            body=messagebody,
            mandatory=self.__mandatory_flag,
            properties=props
        )
        # TODO how to catch channel close!
        # I think has to be caught in basic_publish!

        # (1) Channel closed because even fallback exchange did not exist:
        #     In asynchronous mode: reply_code == 404 and "NOT_FOUND - no exchange 'FALLBACK'" in reply_text)
        #    logerror(LOGGER,'Channel closed because FALLBACK exchange does not exist. Need to close connection to trigger all the necessary close down steps.')
        #    self.thread._connection.close() # This will reconnect!

        # (2) Channel closed because exchange did not exist:
        #     In asynchronous mode: reply_code == 404:
        #    logdebug(LOGGER, 'Channel closed because the exchange "%s" did not exist.', self.__node_manager.get_exchange_name())
        #    self.__use_different_exchange_and_reopen_channel()
        # Then we must set:
        # self.__fallback_exchange = self.__candidate_fallback_exchange_name


    def __avoid_connection_shutdown(self):
        # Source: https://github.com/pika/pika/issues/397
        if (time.time() - self.__connection_last_process_event_call) > 6:
            self.__connection.process_data_events()
            self.__connection_last_process_event_call = time.time()

    '''
    To make sure we use the fallback exchange in case it
    was set, and the normal exchange name otherwise.
    '''
    def __get_exchange_name(self):
        if self.__fallback_exchange is not None:
            return self.__fallback_exchange
        else:
            return self.__nodemanager.get_exchange_name()
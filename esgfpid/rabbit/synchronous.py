import logging
import datetime
import json
import pika
import time
import esgfpid.utils
import esgfpid.defaults
from . import rabbitutils

# Normal logger:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class SynchronousServerConnector(object):

    def __init__(self, **args):
        ''' In init, no connection is opened yet.'''

        # Check args
        mandatory_args = [
            'exchange_name',
            'url_preferred',
            'urls_fallback',
            'credentials'
        ]
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

        self.__store_args(args)
        self.__retrieve_defaults()

        # Other settings:
        self.__channel = None
        self.__connection = None
        self.__communication_established = False
        self.__connection_last_process_event_call = 0
        self.__logdate = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        self.__current_rabbit_url = None
        self.__error_messages_during_init = []

    def __store_args(self, args):
        self.__exchange_name = args['exchange_name']
        self.__credentials = args['credentials']
        self.__url_preferred = args['url_preferred']
        self.__urls_fallback = args['urls_fallback']

    def __retrieve_defaults(self):
        # For sending messages:
        self.__max_tries = esgfpid.defaults.RABBIT_SYN_MAX_TRIES
        self.__timeout_milliseconds = esgfpid.defaults.RABBIT_SYN_TIMEOUT_MILLISEC


    ########################################
    ### This actually connects to rabbit ###
    ########################################

    def open_rabbit_connection(self):
        success = self.__try_connecting_to_preferred()
        if not success:
            success = self.__try_connecting_to_fallbacks()
            if not success:
                collected_errors = ' - '.join(self.__error_messages_during_init)
                LOGGER.warning('No connection possible. Errors: %s' % collected_errors)

    def __try_connecting_to_preferred(self):
        url = self.__url_preferred
        success = self.__try_connecting_to_url(url)
        return success

    def __try_connecting_to_fallbacks(self):
        urls = self.__urls_fallback
        for url in urls:
            success = self.__try_connecting_to_url(url)
            if success:
                return success
        return False

    def __try_connecting_to_url(self, url):

        connection = self.__setup_rabbit_connection(url)

        if connection is not None and connection.is_open:
            self.__connection = connection
            self.__current_rabbit_url = url
            LOGGER.debug('Succeeded opening connection to '+str(url))

            channel = self.__setup_rabbit_channel()

            if channel is not None and channel.is_open:
                LOGGER.debug('Succeeded opening channel.')
                self.__channel = channel
                self.__channel.confirm_delivery()
                self.__communication_established = True
                return True
            else:
                LOGGER.debug('Could not open channel.')

        return False

    def __setup_rabbit_connection(self, url):
        LOGGER.debug('Setting up the connection with the RabbitMQ.')
        try:
            params = esgfpid.rabbit.connparams.get_connection_parameters(self.__credentials, url)
            conn = self.__make_connection(params)
            return conn
        except pika.exceptions.ProbableAuthenticationError:
            msg = ('Problem setting up the rabbit with username "%s" and password "%s" at url %s.' %
                    (self.__credentials.username,
                     self.__credentials.password,
                     url))
            LOGGER.error(msg)
            self.__error_messages_during_init.append(msg)
            return None
        except pika.exceptions.AMQPConnectionError:
            msg = ('Problem setting up the rabbit connection to %s.' % url)
            self.__error_messages_during_init.append(msg)
            return None

    def __make_connection(self, params): # this is easy to mock during unit tests
        return pika.BlockingConnection(params)

    def __setup_rabbit_channel(self):
        try:
            return self.__make_channel()
        except pika.exceptions.ChannelClosed:
            msg = ('Problem setting up the rabbit channel.')
            self.__error_messages_during_init.append(msg)
            LOGGER.warn(msg)
            return None

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

    def send_message_to_queue(self, message):
        self.__open_connection_if_not_open()
        routing_key, msg_string = rabbitutils.get_routing_key_and_string_message_from_message_if_possible(message)
        self.__try_sending_message_several_times(routing_key, msg_string)

    def __open_connection_if_not_open(self):
        if not self.__communication_established:
            self.open_rabbit_connection()

    def __try_sending_message_several_times(self, routing_key, message):
        delivered_after_x_times = self.__retry_x_times(routing_key, message)
        if not delivered_after_x_times:
            LOGGER.error('Message could not be sent.')

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
                LOGGER.debug('Successful delivery of message with routing key "'+str(routing_key)+'"!')
                LOGGER.debug("Delivered after "+str(counter)+" times!")
                message_delivered = True
            else:
                LOGGER.debug("Not delivered, waiting "+str(self.__timeout_milliseconds)+" milliseconds before delivering it again!")
                time.sleep(self.__timeout_milliseconds/1000)

            # Increment counter, check if we have reached the max:
            if counter == self.__max_tries:
                max_tries_reached = True
                LOGGER.debug("Tried "+str(self.__max_tries)+" times, giving up!")
            counter += 1

        # Return with or without success:
        return message_delivered

    def __send_message_to_queue_once(self, routing_key, messagebody):

        props = esgfpid.rabbit.connparams.get_properties_for_message_publications()

        delivered = False
        try:
            delivered = self.__do_send_message(routing_key, messagebody, props)
            self.__avoid_connection_shutdown()
            
        except pika.exceptions.UnroutableError:
            LOGGER.error('Message could not be routed to any queue, maybe none was declared yet.')
            # TODO
        return delivered

    def __do_send_message(self, routing_key, messagebody, props): # This is easy to mock during unit tests
        return self.__channel.basic_publish(
            exchange=self.__exchange_name,
            routing_key=routing_key,
            body=messagebody,
            mandatory=esgfpid.defaults.RABBIT_MANDATORY_DELIVERY,
            properties=props
        )

    def __avoid_connection_shutdown(self):
        # Source: https://github.com/pika/pika/issues/397
        if (time.time() - self.__connection_last_process_event_call) > 6:
            self.__connection.process_data_events()
            self.__connection_last_process_event_call = time.time()

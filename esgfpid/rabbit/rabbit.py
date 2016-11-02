import logging
import datetime
import json
import pika
import time
import random
import esgfpid.utils
import esgfpid.defaults
import esgfpid.rabbit.synchronous
import esgfpid.rabbit.asynchronous
import esgfpid.rabbit.rabbitutils
import esgfpid.rabbit.nodemanager

# Normal logger:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

ASYNCHRONOUS = esgfpid.defaults.RABBIT_IS_ASYNCHRONOUS

class RabbitMessageSender(object):

    def __init__(self, **args):
        LOGGER.debug('Initializing RabbitMessageSender.')

        mandatory_args = [
            'exchange_name',
            'url_preferred',
            'urls_fallback',
            'username',
            'password',
            'test_publication'
        ]
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

        # RabbitMQ Settings
        esgfpid.rabbit.rabbitutils.ensure_urls_are_a_list(args, LOGGER) # TODO: Remove
        esgfpid.rabbit.rabbitutils.ensure_no_duplicate_urls(args, LOGGER) # TODO: Remove
        self.__node_manager = self.__make_rabbit_settings(args)

        self.__test_publication = args['test_publication']
        self.__server_connector = self.__init_server_connector(args, self.__node_manager)


    def __init_server_connector(self, args, node_manager):
        if ASYNCHRONOUS:
            return esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(node_manager)
        else:
            logerror(LOGGER, 'Synchronous communication with RabbitMQ is not supported anymore.')
            raise ValueError('Synchronous communication with RabbitMQ is not supported anymore.')
            # The synchronous module has to be fixed. The way of passing credentials was modified
            # and this was not modified in the synchronous module.
            #return esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)

    def open_rabbit_connection(self):
        if not ASYNCHRONOUS:
            return self.__server_connector.open_rabbit_connection()

    def close_rabbit_connection(self):
        if not ASYNCHRONOUS:
            return self.__server_connector.close_rabbit_connection()

    def finish(self):
        if ASYNCHRONOUS:
            self.__server_connector.finish_rabbit_thread()

    def is_finished(self):
        if ASYNCHRONOUS:
            return self.__server_connector.is_finished()
        else:
            return None

    def start(self):
        if ASYNCHRONOUS:
            self.__server_connector.start_rabbit_thread()

    def force_finish(self):
        if ASYNCHRONOUS:
            self.__server_connector.force_finish_rabbit_thread()

    def any_leftovers(self):
        if ASYNCHRONOUS:
            return self.__server_connector.any_leftovers()

    def get_leftovers(self):
        if ASYNCHRONOUS:
            return self.__server_connector.get_leftovers()

    def send_message_to_queue(self, message):
        if self.__test_publication == True:
            message['test_publication'] = True
        return self.__server_connector.send_message_to_queue(message)

    def __make_rabbit_settings(self, args):

        node_manager = esgfpid.rabbit.nodemanager.NodeManager()

        # Add the trusted node, if there is one:
        if not args['password'] == 'jzlnL78ZpExV#_QHz':
            node_manager.add_trusted_node(
                username=args['username'],
                password=args['password'],
                host=args['url_preferred'],
                exchange_name=args['exchange_name']
            )

        # Open nodes are always added:
        for hostname in args['urls_fallback']:
            node_manager.add_open_node(
                #username=args['username']+'-open',
                username='esgf-publisher-open',
                password='U6-Lke39mN',
                host=hostname,
                exchange_name=args['exchange_name']
            )

        return node_manager


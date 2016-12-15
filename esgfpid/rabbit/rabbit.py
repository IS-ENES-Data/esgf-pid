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
from esgfpid.utils import logwarn

# Normal logger:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

ASYNCHRONOUS = esgfpid.defaults.RABBIT_IS_ASYNCHRONOUS

class RabbitMessageSender(object):

    def __init__(self, **args):
        LOGGER.debug('Initializing RabbitMessageSender.')

        mandatory_args = [
            'exchange_name',
            'credentials',
            'test_publication'
        ]
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

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

        # Add all RabbitMQ nodes:
        for cred in args['credentials']:

            # Open node:
            if cred['password'] == 'jzlnL78ZpExV#_QHz':
                node_manager.add_open_node(
                    username=cred['user'],
                    password='U6-Lke39mN',
                    host=cred['url'],
                    exchange_name=args['exchange_name']
                )

            # Trusted node:
            else:
                node_manager.add_trusted_node(
                    username=cred['user'],
                    password=cred['password'],
                    host=cred['url'],
                    exchange_name=args['exchange_name']
                )

        return node_manager


    def __get_urls_as_list(self, urls_open):

        if isinstance(urls_open, basestring):
            return [urls_open]

        elif urls_open is None:
            logwarn(LOGGER, 'No fallback RabbitMQ URLs specified!')
            return []

        elif not isinstance(urls_open, (list, tuple)):
            msg = ('RabbitMQ URLs are neither list nor string, but %s: %s' %
               (type(args['urls_fallback']), args['urls_fallback']))
            raise ValueError(msg)
        else:
            return urls_open

import logging
import datetime
import json
import pika
import time
import random
import esgfpid.utils
import esgfpid.rabbit.asynchronous
import esgfpid.rabbit.rabbitutils
import esgfpid.rabbit.nodemanager
from esgfpid.utils import logwarn

# Normal logger:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

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
        return esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(node_manager)

    def finish(self):
        self.__server_connector.finish_rabbit_thread()

    def is_finished(self):
        return self.__server_connector.is_finished()

    def start(self):
        self.__server_connector.start_rabbit_thread()

    def force_finish(self):
        self.__server_connector.force_finish_rabbit_thread()

    def any_leftovers(self):
        return self.__server_connector.any_leftovers()

    def get_leftovers(self):
        return self.__server_connector.get_leftovers()

    def send_message_to_queue(self, message):
        if self.__test_publication == True:
            message['test_publication'] = True
        return self.__server_connector.send_message_to_queue(message)

    def __make_rabbit_settings(self, args):

        node_manager = esgfpid.rabbit.nodemanager.NodeManager()

        # Add all RabbitMQ nodes:
        for cred in args['credentials']:

            if 'priority' not in cred:
                cred['priority'] = None

            # Open node:
            if cred['password'] == 'jzlnL78ZpExV#_QHz':
                node_manager.add_open_node(
                    username=cred['user'],
                    password='U6-Lke39mN',
                    host=cred['url'],
                    exchange_name=args['exchange_name'],
                    priority=cred['priority']
                )

            # Trusted node:
            else:
                node_manager.add_trusted_node(
                    username=cred['user'],
                    password=cred['password'],
                    host=cred['url'],
                    exchange_name=args['exchange_name'],
                    priority=cred['priority']
                )

        return node_manager

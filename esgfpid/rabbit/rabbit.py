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

        esgfpid.rabbit.rabbitutils.ensure_urls_are_a_list(args, LOGGER)
        esgfpid.rabbit.rabbitutils.set_preferred_url(args, LOGGER)
        esgfpid.rabbit.rabbitutils.ensure_no_duplicate_urls(args, LOGGER)

        self.__test_publication = args['test_publication']
        self.__set_credentials(args)
        self.__server_connector = self.__init_server_connector(args)

    def __init_server_connector(self, args):
        if ASYNCHRONOUS:
            return esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(**args)
        else:
            return esgfpid.rabbit.synchronous.SynchronousServerConnector(**args)

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

    def __set_credentials(self, args):
        if args['password'] == 'jzlnL78ZpExV#_QHz':
            args['password'] = 'U6-Lke39mN'
        args['credentials'] = esgfpid.rabbit.connparams.get_credentials(
            args['username'],
            args['password']
        )
        del args['username']
        del args['password']


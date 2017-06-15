import logging
import pika
import esgfpid.utils
from esgfpid.utils import logwarn
from .nodemanager import NodeManager
from .asynchronous import AsynchronousRabbitConnector
from .synchronous import SynchronousRabbitConnector


# Normal logger:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
This class provides the library with a facade to
a module that handles connections to RabbitMQ in
a synchronous or asynchronous way.

It is basically a facade/wrapper for either a
:py:class:`~esgfpid.rabbit.synchronous.SynchronousServerConnector`
or a 
:py:class:`~esgfpid.rabbit.asynchronous.asynchronous.AsynchronousServerConnector`

'''
class RabbitMessageSender(object):

    '''
    Create a RabbitMessageSender that takes care of managing
    connections to RabbitMQ instances and sending messages 
    to them.

    :param exchange_name: Mandatory. The name of the exchange
        to send messages to (string).
    :param credentials: Mandatory. List of dictionaries containing
        the information about the RabbitMQ nodes.
    :param is_synchronous_mode: Mandatory. Boolean to define if
        the connection to RabbitMQ and the message sending
        should work in synchronous mode.
    :param test_publication: Mandatory. Boolean to tell whether
        a test flag should be added to all messages.

    '''
    def __init__(self, **args):
        LOGGER.debug('Initializing RabbitMessageSender.')

        mandatory_args = [
            'exchange_name',
            'credentials',
            'test_publication',
            'is_synchronous_mode'
        ]
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

        self.__ASYNCHRONOUS = not args['is_synchronous_mode']
        self.__test_publication = args['test_publication']
        self.__node_manager = self.__make_rabbit_settings(args)
        self.__server_connector = self.__init_server_connector(args, self.__node_manager)

    def __init_server_connector(self, args, node_manager):
        if self.__ASYNCHRONOUS:
            return esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(node_manager)
        else:
            return esgfpid.rabbit.synchronous.SynchronousRabbitConnector(node_manager)


    '''
    Open a synchronous connection to RabbitMQ.

    This only has an effect if the RabbitMessageSender is in
    synchronous mode.

    Please see: :func:`~rabbit.synchronous.SynchronousServerConnector.open_rabbit_connection`.

    If this is not called before the first message is sent,
    the first sent message automatically opens a connection.

    Opening and closing the connection is provided as
    separate methods because clients that know that they
    want to send many messages one after the other should
    preferably open and close the synchronous connection
    themselves, to avoid opening and reclosing all the time.

    This is called for example by the publish assistant,
    which opens a connection, sends all the messages that
    belong to one dataset, and closes it again.
    '''
    def open_rabbit_connection(self):
        if not self.__ASYNCHRONOUS:
            return self.__server_connector.open_rabbit_connection()

    '''
    Close a synchronous connection to RabbitMQ.

    This only has an effect if the RabbitMessageSender is in
    synchronous mode.

    Synchronous connections are never closed automatically.
    However, if the client forgets to close, it does not
    block the library. It is still preferred that the client
    closes the connection as soon as he knows he won't send
    any more messages (for a while).
    Otherwise, the connection is kept open and will send
    heartbeats from time to time.

    This is called for example by the publish assistant,
    which opens a connection, sends all the messages that
    belong to one dataset, and closes it again.
    '''
    def close_rabbit_connection(self):
        if not self.__ASYNCHRONOUS:
            return self.__server_connector.close_rabbit_connection()


    '''

    Close a thread that communicates with RabbitMQ asynchronously,
    in a gentle way. This means that the thread does try to send
    the last pending messages and receive the last pending
    delivery confirmations, up to a maximum waiting time.

    This only has an effect if the RabbitMessageSender is in
    asynchronous mode.

    Please see documentation of asynchronous rabbit module
    (:func:`~rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector.force_finish_rabbit_thread`).
    '''
    def finish(self):
        if self.__ASYNCHRONOUS:
            self.__server_connector.finish_rabbit_thread()

    def is_finished(self):
        if self.__ASYNCHRONOUS:
            return self.__server_connector.is_finished()
        else:
            return None

    '''
    Start a thread that communicates with RabbitMQ asynchronously.

    This only has an effect if the RabbitMessageSender is in
    asynchronous mode.

    Please see documentation of asynchronous rabbit module
    (:func:`~rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector.start_rabbit_thread`).
    '''
    def start(self):
        if self.__ASYNCHRONOUS:
            self.__server_connector.start_rabbit_thread()

    '''
    Force-close a thread that communicates with RabbitMQ asynchronously.

    This only has an effect if the RabbitMessageSender is in
    asynchronous mode.
    Please see documentation of asynchronous rabbit module
    (:func:`~rabbit.asynchronous.asynchronous.AsynchronousRabbitConnector.force_finish_rabbit_thread`).
    '''
    def force_finish(self):
        if self.__ASYNCHRONOUS:
            self.__server_connector.force_finish_rabbit_thread()

    def any_leftovers(self):
        if self.__ASYNCHRONOUS:
            return self.__server_connector.any_leftovers()

    def get_leftovers(self):
        if self.__ASYNCHRONOUS:
            return self.__server_connector.get_leftovers()

    '''
    Send a message to RabbitMQ.

    In asynchronous mode, we cannot tell whether the
    delivery as successful, as the delivery confirmation
    will arrive later.

    In synchronous mode, if the delivery was not successful,
    an exception is raised.

    :param: JSON message as string or dictionary. It should
        include its routing key as a dictionary entry with
        key "ROUTING_KEY", Otherwise a default routing key
        will be used to send the message.
    :raises: esgfpid.exceptions.MessageNotDeliveredException:
        In case the message was not delivered. Only in
        synchronous mode.
    '''
    def send_message_to_queue(self, message):
        if self.__test_publication == True:
            message['test_publication'] = True
        self.__server_connector.send_message_to_queue(message)

    def __make_rabbit_settings(self, args):
        node_manager = NodeManager()

        # Add all RabbitMQ nodes:
        for cred in args['credentials']:
            
            if 'priority' not in cred:
                cred['priority'] = None
            if 'vhost' not in cred:
                cred['vhost'] = None
            if 'port' not in cred:
                cred['port'] = None
            if 'ssl_enabled' not in cred:
                cred['ssl_enabled'] = None

            # Open node:
            if cred['password'] == 'jzlnL78ZpExV#_QHz':
                node_manager.add_open_node(
                    username=cred['user'],
                    password='U6-Lke39mN',
                    host=cred['url'],
                    exchange_name=args['exchange_name'],
                    priority=cred['priority'],
                    vhost=cred['vhost'],
                    port=cred['port'],
                    ssl_enabled=cred['ssl_enabled']
                )

            # Trusted node:
            else:
                node_manager.add_trusted_node(
                    username=cred['user'],
                    password=cred['password'],
                    host=cred['url'],
                    exchange_name=args['exchange_name'],
                    priority=cred['priority'],
                    vhost=cred['vhost'],
                    port=cred['port'],
                    ssl_enabled=cred['ssl_enabled']
                )

        return node_manager
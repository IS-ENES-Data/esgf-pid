import logging
import esgfpid.utils
from esgfpid.rabbit.asynchronous import AsynchronousRabbitConnector
import esgfpid.rabbit.nodemanager
from esgfpid.utils import logwarn

# Normal logger:
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
This is the main module for communications between
the library and the RabbitMQ. All the communication
with the RabbitMQ servers are done by this module
and its submodules.

The methods of this module are called by the coupling
module:
  - start()
  - finish()
  - force_finish()
  - send_message_to_queue()
'''
class RabbitMessageSender(object):

    def __init__(self, **args):
        LOGGER.debug('Initializing RabbitMessageSender.')
        mandatory_args = ['exchange_name', 'credentials', 'test_publication']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)


        '''
        The node manager holds and manages all the
        RabbitMQ server instances, their URLs, their
        credentials, etc.
        '''
        self.__node_manager = self.__make_rabbit_settings(args)

        '''
        TODO
        '''
        self.__test_publication = args['test_publication']

        '''
        This creates the object that does
        the actual communications.
        '''
        self.__server_connector = AsynchronousRabbitConnector(__node_manager)


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

    ###########################################
    ### API towards the library             ###
    ### (methods called by coupling module) ###
    ###########################################

    def start(self):
        self.__server_connector.start_rabbit_thread()

    def finish(self):
        self.__server_connector.finish_rabbit_thread()

    def force_finish(self):
        self.__server_connector.force_finish_rabbit_thread()

    def send_message_to_queue(self, message):
        if self.__test_publication == True:
            message['test_publication'] = True
        self.__server_connector.send_message_to_queue(message)

    

import logging
import esgfpid.utils
from esgfpid.rabbit.asynchronous import AsynchronousRabbitConnector
import esgfpid.rabbit.nodemanager
from esgfpid.utils import logwarn
from .asynchronous.exceptions import OperationNotAllowed

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
        Adds a test flag to all test publications.
        '''
        self.__test_publication = args['test_publication']

        '''
        This creates the object that does
        the actual communications.
        '''
        self.__server_connector = AsynchronousRabbitConnector(self.__node_manager)

        self.__state = self.__state_not_started
        self.NOT_STARTED = 0
        self.CALLED_START = 1
        self.CALLED_FINISH = 2


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

        if self.__state == self.NOT_STARTED:
            self.__start()

        elif self.__state == self.CALLED_START:
            LOGGER.debug('Calling start again has no effect.')

        elif self.__state == self.CALLED_FINISH:
            LOGGER.debug('Calling start after having called finish has no effect.')

    def __start(self):
        self.__state = self.CALLED_START
        self.__server_connector.start_rabbit_thread()

    def finish(self):
        if self.__state == self.NOT_STARTED:
            LOGGER.debug('Calling finish without having started has no effect.')

        elif self.__state == self.CALLED_START:
            self.__finish()

        elif self.__state == self.CALLED_FINISH:
            LOGGER.debug('Calling finish again has no effect.')

    def __finish(self):
        self.__state = self.CALLED_FINISH
        self.__server_connector.finish_rabbit_thread()

    def force_finish(self):
        if self.__state == self.NOT_STARTED:
            LOGGER.debug('Calling finish without having started has no effect.')

        elif self.__state == self.CALLED_START or self.__state == self.CALLED_FINISH:
            self.__force_finish()

    def __force_finish(self):
        self.__state = self.CALLED_FINISH
        self.__server_connector.force_finish_rabbit_thread()

    def send_message_to_queue(self, message):

        if self.__state == self.NOT_STARTED:
            msg = 'Cannot send requests if thread was not started!'
            raise OperationNotAllowed(msg)

        elif self.__state == self.CALLED_FINISH:
            msg = 'Cannot send requests if thread was stopped!'
            raise OperationNotAllowed(msg)

        elif self.__state == self.CALLED_START:
            self.__send_message_to_queue(message)
            

    def __send_message_to_queue(self, message):
        if self.__test_publication == True:
            message['test_publication'] = True
        self.__server_connector.send_message_to_queue(message)

    

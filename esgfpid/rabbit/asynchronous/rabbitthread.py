import Queue
import threading
import pika
import time
import copy
import datetime
import logging
import esgfpid.utils
from .thread_acceptor import PublicationReceiver
from .thread_statemachine import StateMachine
from .thread_builder import ConnectionBuilder
from .thread_feeder import RabbitFeeder
from .thread_shutter import ShutDowner
from .thread_confirmer import ConfirmReactor
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from .exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class RabbitThread(threading.Thread):

    def __init__(self, **args):

        threading.Thread.__init__(self)
        logdebug(LOGGER, 'init:Initializing rabbit connector class...')

        mandatory_args = ['url_preferred', 'exchange_name', 'credentials']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

        # pika objects
        self._connection = None # pika.SelectConnection
        self._channel = None    # pika.channel.Channel

        # Error codes
        self.ERROR_CODE_CONNECTION_CLOSED_BY_USER=999
        self.ERROR_TEXT_CONNECTION_FORCE_CLOSED='(forced finish)'
        self.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN='(not reopen)'

        # Submodules
        self.statemachine = StateMachine()
        self.confirmer = ConfirmReactor()
        self.feeder = RabbitFeeder(self, self.statemachine, self.confirmer, args['exchange_name'])
        self.acceptor = PublicationReceiver(self, self.statemachine, self.feeder)
        self.shutter = ShutDowner(self, self.statemachine, self.confirmer, self.feeder)
        self.builder = ConnectionBuilder(self, self.statemachine, self.confirmer, self.feeder, self.acceptor, self.shutter, args)


        # Loggin
        #self.__inform_about_settings()
        logdebug(LOGGER, 'init:Initializing rabbit connector class... done.')

    ######################################
    ### Start the thread, i.e.         ###
    ### start waiting for publications ###
    ######################################

    def run(self):
        logdebug(LOGGER, 'Starting thread...')
        logdebug(LOGGER, 'Trigger connection to rabbit...', show=True)
        self.builder.trigger_connection_to_rabbit_etc()
        logdebug(LOGGER, 'Trigger connection to rabbit... done.', show=True)
        logdebug(LOGGER, 'Starting thread done...')
        logdebug(LOGGER, 'Starting ioloop...', show=True)
        self.builder.start_ioloop_when_connection_ready()
        logtrace(LOGGER, 'Had started ioloop, but its already closed again.')


    #############################################
    ### Publisher hands message to thread     ###
    ### (Putting messages into the Queue      ###
    ###  and triggering publication actions.) ###
    #############################################

    def send_a_message(self, message):
        self.acceptor.send_a_message(message)

    def send_many_messages(self, messages):
        self.acceptor.send_many_messages(messages)

    ####################
    ### Force finish ###
    ####################

    def force_finish(self, msg='Forced finish from unknown source'):
        self.shutter.force_finish(msg)

    #####################
    ### Gentle finish ###
    #####################

    def finish_gently(self):
        self.shutter.finish_gently()

    ###############
    ### Getters ###
    ###############

    def get_unpublished_messages_as_list(self):
        if not self.is_alive():
            return self.feeder.get_unpublished_messages_as_list_copy()
        else:
            raise OperationNotAllowed('thread has not finished','retrieving unpublished messages')

    def get_unconfirmed_messages_as_list(self):
        if not self.is_alive():
            return self.confirmer.get_unconfirmed_messages_as_list_copy()
        else:
            raise OperationNotAllowed('thread has not finished','retrieving unconfirmed messages')

    def get_nacked_messages_as_list(self):
        if not self.is_alive():
            return self.confirmer.get_copy_of_nacked()
        else:
            raise OperationNotAllowed('thread has not finished','retrieving nacked messages')
 
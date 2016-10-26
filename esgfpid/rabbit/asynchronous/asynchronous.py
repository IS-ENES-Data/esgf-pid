import Queue
import threading
import pika
import time
import json
import copy
import datetime
import logging
import esgfpid.utils
from .rabbitthread import RabbitThread
from .exceptions import ConnectionNotReady
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class AsynchronousRabbitConnector(object):

    def __init__(self, **args):

        self.__check_args(args)
        self.__args = args # Store args for when thread is started

        self.__leftovers_unpublished = [] # will be filled after join
        self.__leftovers_unconfirmed = [] # will be filled after join
        self.__leftovers_nacked      = [] # will be filled after join
        self.__not_started_yet = True

    def __check_args(self, args):
        mandatory_args = [
            'exchange_name',
            'url_preferred',
            'urls_fallback',
            'credentials'
        ]
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

    def start_rabbit_thread(self):
        self.__not_started_yet = False
        self.rabbit_thread = self.__create_thread(self.__args)
        self.rabbit_thread.start()
        self.__publish_leftovers()

    def __create_thread(self, args):
        return RabbitThread(**args)

    def send_message_to_queue(self, message):
        if self.__not_started_yet:
            msg = ('Cannot publish message. The message sending module was not initalized yet. '+
                   '(Please call the PID connector\'s "start_messaging_thread()" before trying '+
                   'to send messages, and do not forget to "finish_messaging_thread()" afterwards.')
            raise ValueError(msg)
        self.rabbit_thread.send_a_message(message)

    def send_many_messages_to_queue(self, list_of_messages):
        if self.__not_started_yet:
            msg = ('Cannot publish message. The message sending module was not initalized yet. '+
                   '(Please call the PID connector\'s "start_messaging_thread()" before trying '+
                   'to send messages, and do not forget to "finish_messaging_thread()" afterwards.')
            raise ValueError(msg)
        self.rabbit_thread.send_many_messages(list_of_messages)

    def reconnect(self):
        self.start_rabbit_thread()

    #################
    ### Finishing ###
    #################

    def is_finished(self):
        return (not self.rabbit_thread.is_alive())

    def finish_rabbit_thread(self):
        self.__finish_gently()
        self.__join_and_rescue()

    def force_finish_rabbit_thread(self, msg=None):
        self.__force_finish(msg)
        self.__join_and_rescue()

    def __join_and_rescue(self):
        success = self.__join()
        if success:
            self.__rescue()
        else:
            for i in xrange(10):
                time.sleep(1)
            self.__force_finish('Join failed once.')
            success = self.__join()
            if success:
                self.__rescue()
            else:
                LOGGER.error('Joining failed again. No idea why.')

    def __finish_gently(self):
        logdebug(LOGGER, 'Finishing...')
        self.rabbit_thread.finish_gently()
        logdebug(LOGGER, 'Finishing... done')

    def __force_finish(self, msg):
        logdebug(LOGGER, 'Force finishing...')
        if msg is None:
            msg = 'force finish called by library'
        self.rabbit_thread.force_finish(msg)
        logdebug(LOGGER, 'Force finishing... done')

    def __join(self):        
        logdebug(LOGGER, 'Joining...')
        timeout_seconds=2
        self.rabbit_thread.join(timeout_seconds)
        if self.rabbit_thread.is_alive():
            logdebug(LOGGER, 'Joining failed.')
            return False
        else:
            logdebug(LOGGER, 'Joining... done')
            return True

    def __rescue(self):
        logdebug(LOGGER, 'Storing unpublished/unconfirmed messages...')
        self.__rescue_leftovers()
        logdebug(LOGGER, 'Storing unpublished/unconfirmed messages... done.')      

    ##############################################
    ### Unpublished/unconfimed/nacked messages ###
    ##############################################

    def __rescue_leftovers(self):
        self.__rescue_unpublished_messages()
        self.__rescue_nacked_messages()
        self.__rescue_unconfirmed_messages()

    def __rescue_unpublished_messages(self):
        if self.rabbit_thread.is_alive():
            logwarn(LOGGER, ''Cannot retrieve unpublished messages while thread still alive.')
        else:
            self.__leftovers_unpublished = self.rabbit_thread.get_unpublished_messages_as_list()
            num = len(self.__leftovers_unpublished)
            if num > 0:
                logdebug(LOGGER, 'Rescued %i unpublished messages.', num)
            else:
                logdebug(LOGGER, 'No unpublished messages to rescue.')

    def __rescue_unconfirmed_messages(self):
        if self.rabbit_thread.is_alive():
            logwarn(LOGGER, ''Cannot retrieve unconfirmed messages while thread still alive.')
        else:
            self.__leftovers_unconfirmed = self.rabbit_thread.get_unconfirmed_messages_as_list()
            num = len(self.__leftovers_unconfirmed)
            if num > 0:
                logdebug(LOGGER, 'Rescued %i unconfirmed messages.', num)
            else:
                logdebug(LOGGER, 'No unconfirmed messages to rescue.')

    def __rescue_nacked_messages(self):
        if self.rabbit_thread.is_alive():
            logwarn(LOGGER, ''Cannot retrieve rejected (NACKed) messages while thread still alive.')
        else:
            self.__leftovers_nacked = self.rabbit_thread.get_nacked_messages_as_list()
            num = len(self.__leftovers_nacked)
            if num > 0:
                logdebug(LOGGER, 'Rescued %i rejected (NACKed) messages.', num)
            else:
                logdebug(LOGGER, 'No rejected (NACKed) messages to rescue.')

    def __publish_leftovers(self): # after reconnect
        self.__publish_leftovers_unpublished()
        self.__publish_leftovers_unconfirmed()
        self.__publish_leftovers_nacked()

    def __publish_leftovers_unpublished(self):
        if len(self.__leftovers_unpublished) > 0:
            self.rabbit_thread.send_many_messages(self.__leftovers_unpublished)
            self.__leftovers_unpublished = []

    def __publish_leftovers_unconfirmed(self):
        if len(self.__leftovers_unconfirmed) > 0:
            self.rabbit_thread.send_many_messages(self.__leftovers_unconfirmed)
            self.__leftovers_unconfirmed = []

    def __publish_leftovers_nacked(self):
        if len(self.__leftovers_nacked) > 0:
            self.rabbit_thread.send_many_messages(self.__leftovers_nacked)
            self.__leftovers_nacked = []

    def any_leftovers(self):
        if (len(self.__leftovers_unpublished)+
            len(self.__leftovers_unconfirmed)+
            len(self.__leftovers_nacked))>0:
            return True
        return False

    def get_leftovers(self):
        return self.__leftovers_unpublished + self.__leftovers_unconfirmed + self.__leftovers_nacked
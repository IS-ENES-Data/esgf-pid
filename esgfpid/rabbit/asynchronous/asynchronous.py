'''
This module is responsible for all the communication with the RabbitMQ
messaging service. It provides one class, the "AsynchronousRabbitConnector".

It communicates with the server in an asychronous way, by opening
another thread which runs in parallel, waits for events from the library
(e.g. message publication requests) and from the RabbitMQ (e.g. message
receival confirmations).

After initialisation, it is necessary to start the thread by calling
"start_messaging_thread()". This opens the parallel thread and triggers 
the connection to the server.

Then, the messages can be sent using "send_message_to_queue()" or
"send_many_messages()".

After the messaging business is done, it is necessary to close the
thread by calling "finish_rabbit_thread()" or "force_finish_rabbit_thread()".

  .. note:: Please note that KeyboardInterrupts may not work while a
    second thread is running. They need to be explicitly caught in
    the code calling the esgfpid library.

'''

import Queue
import threading
import pika
import time
import json
import copy
import datetime
import logging
import esgfpid.utils
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from .rabbitthread import RabbitThread
from .thread_statemachine import StateMachine
from .exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class AsynchronousRabbitConnector(object):

    '''
    Constructor for the asychronous rabbit connection module.

    This does not open a connection or start a thread yet.

    :param node_manager: NodeManager object that contains 
        the info about all the available RabbitMQ instances,
        their credentials, their priorities.

    '''
    def __init__(self, node_manager):
        logdebug(LOGGER, 'Initializing rabbit connector...')

        '''
        To check whether the thread has been started yet.
        If not, the methods of this module will raise an error, to
        make sure that the library caller will start the thread.
        '''
        self.__not_started_yet = True

        # To be filled after join:
        self.__leftovers_unpublished = [] # will be filled after join
        self.__leftovers_unconfirmed = [] # will be filled after join
        self.__leftovers_nacked      = [] # will be filled after join

        # Shared objects
        self.__statemachine = StateMachine()
        self.__unpublished_messages_queue = Queue.Queue()

        # Log flags
        self.__first_message_receival = True
        self.__logcounter_received = 1
        self.__LOGFREQUENCY = 10

        # Actually created the thread:
        #self.__thread = RabbitThread(self.__statemachine, self.__unpublished_messages_queue, self, node_manager)
        self.__thread = self.__create_thread(node_manager)

        logdebug(LOGGER, 'Initializing rabbit connector... done.')

    def __create_thread(self, node_manager): # easy to mock/patch in unit test!
        return RabbitThread(self.__statemachine, self.__unpublished_messages_queue, self, node_manager)


    '''
    This starts the parallel thread.
    Needs to be called manually by library user.

    We would like to encourage the library user to start the
    thread as early as possible, even before using the library
    to publish messages.
    This is only possible if the thread can be started (and
    stopped) separately from the rest of the library's functionality.

    Reasons for this method not to be called during init of
    the library may be:

    * Some usage may not need any connection, e.g. if the library is only used for creating handle strings.
    * Testing of the library is easier.
    * If starting needs to be called explicitly, it is less likely that the stopping is forgotten, so abandoned connections and unjoined threads are avoided.

    '''
    def start_rabbit_thread(self):
        self.__not_started_yet = False
        self.__statemachine.set_to_waiting_to_be_available()
        self.__thread.start()

    #################
    ### Finishing ###
    #################

    def is_finished(self):
        return (not self.__thread.is_alive())

    '''
    "Gentle" finish of the thread.
    If any messages are not published or confirmed yet, the
    thread waits and rechecks for some time.

    So this message may block while the rabbit thread waits
    for some pending messages, and it may block while the
    main thread tries to join the rabbit thread.
    '''
    def finish_rabbit_thread(self):
        logdebug(LOGGER, 'Finishing...')

        # Make sure no more messages are accepted from publisher
        # while publishes/confirms are still accepted:
        if self.__statemachine.is_AVAILABLE():
            self.__statemachine.set_to_wanting_to_stop()
        self.__statemachine.detail_asked_to_gently_close_by_publisher = True

        # Asking the thread to finish:
        self.__thread.add_event_gently_finish() # (this blocks!)
        logdebug(LOGGER, 'Finishing... done')
        self.__join_and_rescue()

    '''
    Forces the immediate close-down of the thread, no matter
    if messages are still pending (i.e. not published or not
    confirmed).

    Note: If this method blocks, it is because the joining
    of the thread blocks.
    '''
    def force_finish_rabbit_thread(self):
        logdebug(LOGGER, 'Force finishing...')

        # Make sure no more messages are accepted from publisher
        # while confirms are still accepted:
        self.__statemachine.detail_asked_to_force_close_by_publisher = True
        self.__statemachine.set_to_wanting_to_stop()

        # Asking the thread to finish:
        logwarn(LOGGER, 'Forced close down of message sending module. Will not wait for pending messages, if any.')
        self.__thread.add_event_force_finish()
        logdebug(LOGGER, 'Force finishing... done')
        self.__join_and_rescue()

    '''
    Tries several times to join the thread.
    Note: May block for up to 6 seconds!
    '''
    def __join_and_rescue(self):
        timeout_seconds = 2
        success = self.__join(timeout_seconds)
        if success:
            self.__rescue_leftovers()
        else:
            loginfo(LOGGER, 'Joining the thread failed once... Retrying.')
            for i in xrange(20):
                time.sleep(0.1) # blocking
            self.__thread.add_event_force_finish()
            success = self.__join(timeout_seconds)
            if success:
                self.__rescue_leftovers()
            else:
                logerror(LOGGER, 'Joining failed again. No idea why.')

    def __join(self, timeout_seconds):        
        logdebug(LOGGER, 'Joining...')
        self.__thread.join(timeout_seconds)
        if self.__thread.is_alive():
            logdebug(LOGGER, 'Joining failed.')
            return False
        else:
            logdebug(LOGGER, 'Joining... done')
            return True  

    ##############################################
    ### Unpublished/unconfimed/nacked messages ###
    ##############################################

    '''
    Stores the pending messages in variables after
    closing the rabbit thread.
    So far, nothing is done with them, as republishing
    them does not work after closing, and writing to
    console or to file makes no sense.
    '''
    def __rescue_leftovers(self):
        logdebug(LOGGER, 'Storing unpublished/unconfirmed messages...')
        self.__rescue_unpublished_messages()
        self.__rescue_nacked_messages()
        self.__rescue_unconfirmed_messages()
        logdebug(LOGGER, 'Storing unpublished/unconfirmed messages... done.')      

    def __rescue_unpublished_messages(self):
        if self.__thread.is_alive():
            logwarn(LOGGER, 'Cannot retrieve unpublished messages while thread still alive.')
        else:
            self.__leftovers_unpublished = self.__get_unpublished_messages_as_list()
            num = len(self.__leftovers_unpublished)
            if num > 0:
                logdebug(LOGGER, 'Rescued %i unpublished messages.', num)
            else:
                logdebug(LOGGER, 'No unpublished messages to rescue.')

    def __rescue_unconfirmed_messages(self):
        if self.__thread.is_alive():
            logwarn(LOGGER, 'Cannot retrieve unconfirmed messages while thread still alive.')
        else:
            self.__leftovers_unconfirmed = self.__get_unconfirmed_messages_as_list()
            num = len(self.__leftovers_unconfirmed)
            if num > 0:
                logdebug(LOGGER, 'Rescued %i unconfirmed messages.', num)
            else:
                logdebug(LOGGER, 'No unconfirmed messages to rescue.')

    def __rescue_nacked_messages(self):
        if self.__thread.is_alive():
            logwarn(LOGGER, 'Cannot retrieve rejected (NACKed) messages while thread still alive.')
        else:
            self.__leftovers_nacked = self.__get_nacked_messages_as_list()
            num = len(self.__leftovers_nacked)
            if num > 0:
                logdebug(LOGGER, 'Rescued %i rejected (NACKed) messages.', num)
            else:
                logdebug(LOGGER, 'No rejected (NACKed) messages to rescue.')

    '''
    This code is not currently used, as the library does not provide
    any possibility to reconnect manually.
    The republication of unpublished/unconfirmed messages after a
    connection interruption is handled by the RabbitThread itself.

    def __publish_leftovers(self):
        self.__publish_leftovers_unpublished()
        self.__publish_leftovers_unconfirmed()
        self.__publish_leftovers_nacked()

    def __publish_leftovers_unpublished(self):
        if len(self.__leftovers_unpublished) > 0:
            self.__thread.send_many_messages(self.__leftovers_unpublished)
            self.__leftovers_unpublished = []

    def __publish_leftovers_unconfirmed(self):
        if len(self.__leftovers_unconfirmed) > 0:
            self.__thread.send_many_messages(self.__leftovers_unconfirmed)
            self.__leftovers_unconfirmed = []

    def __publish_leftovers_nacked(self):
        if len(self.__leftovers_nacked) > 0:
            self.__thread.send_many_messages(self.__leftovers_nacked)
            self.__leftovers_nacked = []
    '''

    #def any_leftovers(self):
    #    if (len(self.__leftovers_unpublished)+
    #        len(self.__leftovers_unconfirmed)+
    #        len(self.__leftovers_nacked))>0:
    #        return True
    #    return False

    #def get_leftovers(self):
    #    return self.__leftovers_unpublished + self.__leftovers_unconfirmed + self.__leftovers_nacked


    ####################
    # Sending messages #
    ####################

    '''
    Send a JSON message to RabbitMQ.

    :param message: JSON message to be published.
    :raises: OperationNotAllowed: If the rabbit thread was not started yet
    or stopped again.
    '''
    def send_message_to_queue(self, message):
        if self.__not_started_yet:
            msg = ('Cannot publish message. The message sending module was not initalized yet. '+
                   '(Please call the PID connector\'s "start_messaging_thread()" before trying '+
                   'to send messages, and do not forget to "finish_messaging_thread()" afterwards.')
            raise OperationNotAllowed(msg)
            # Note: This exception is only thrown if the code that calls the library does
            # it wrong. So we throw this exception to remind the developer to start the rabbit
            # before using it.
        self.__send_a_message(message)

    '''
    Send many JSON messages to RabbitMQ.

    :param list_of_messages: List of JSON message to be published.
    :raises: OperationNotAllowed: If the rabbit thread was not started yet
    or stopped again.
    '''
    def send_many_messages_to_queue(self, list_of_messages):
        if self.__not_started_yet:
            msg = ('Cannot publish message. The message sending module was not initalized yet. '+
                   '(Please call the PID connector\'s "start_messaging_thread()" before trying '+
                   'to send messages, and do not forget to "finish_messaging_thread()" afterwards.')
            raise OperationNotAllowed(msg)
        self.__send_many_messages(list_of_messages)

    def __send_a_message(self, message):
        if self.__statemachine.is_WAITING_TO_BE_AVAILABLE():
            self.__log_receival_one_message(message)
            self.__put_one_message_into_queue_of_unsent_messages(message)

        elif self.__statemachine.is_AVAILABLE():
            self.__log_receival_one_message(message)
            self.__put_one_message_into_queue_of_unsent_messages(message)
            self.__trigger_one_publish_action()

        elif self.__statemachine.is_AVAILABLE_BUT_WANTS_TO_STOP() or self.__statemachine.is_PERMANENTLY_UNAVAILABLE() or self.__statemachine.is_FORCE_FINISHED():
            errormsg = 'Accepting no more messages'
            logdebug(LOGGER, errormsg+' (dropping %s).', message)
            logwarn(LOGGER, 'RabbitMQ module was closed and does not accept any more messages. Dropping message. Reason: %s', self.__statemachine.get_reason_shutdown())
            # Note: This may happen if the connection failed. We may not stop
            # the publisher in this case, so we do not raise an exception.
            # We only raise an exception if the closing was asked by the publisher!
            if self.__statemachine.get_detail_closed_by_publisher():
                raise OperationNotAllowed(errormsg)

        elif self.__statemachine.is_NOT_STARTED_YET():
            errormsg = 'Cannot send a message, the messaging thread was not started yet!'
            logwarn(LOGGER, errormsg+' (dropping %s).', message)
            raise OperationNotAllowed(errormsg)
            # This is almost the same as the one raised if self.__not_started_yet is True.


    def __send_many_messages(self, messages):
        if self.__statemachine.is_WAITING_TO_BE_AVAILABLE():
            self.__log_receival_many_messages(messages)
            self.__put_all_messages_into_queue_of_unsent_messages(messages)

        elif self.__statemachine.is_AVAILABLE():
            self.__log_receival_many_messages(messages)
            self.__put_all_messages_into_queue_of_unsent_messages(messages)
            self.__trigger_n_publish_actions(len(messages))

        elif self.__statemachine.is_AVAILABLE_BUT_WANTS_TO_STOP() or self.__statemachine.is_PERMANENTLY_UNAVAILABLE() or self.__statemachine.is_FORCE_FINISHED():
            errormsg = 'Accepting no more messages'
            logwarn(LOGGER, errormsg+' (dropping %i messages).', len(messages))
            if self.__statemachine.get_detail_closed_by_publisher():
                raise OperationNotAllowed(errormsg)

        elif self.__statemachine.is_NOT_STARTED_YET():
            errormsg = 'Cannot send any messages, the messaging thread was not started yet!'
            logwarn(LOGGER, errormsg+' (dropping %i messages).', len(messages))
            raise OperationNotAllowed(errormsg)

    def __put_one_message_into_queue_of_unsent_messages(self, message):
        logtrace(LOGGER, 'Putting a message into stack that waits to be published...')
        self.__unpublished_messages_queue.put(message, block=False)

    def __log_receival_one_message(self, message):
        if self.__first_message_receival:
            logdebug(LOGGER, 'Handing over first message to rabbit thread...')
            self.__first_message_receival = False
        logtrace(LOGGER, 'Handing over one message over to the rabbit thread (%s)', message)
        log_every_x_times(LOGGER, self.__logcounter_received, self.__LOGFREQUENCY, 'Handing over one message over to the rabbit thread (no. %i).', self.__logcounter_received)
        self.__logcounter_received += 1

    def __log_receival_many_messages(self, messages):
        if self.__first_message_receival:
            logdebug(LOGGER, 'Handing over first message to rabbit thread...')
            self.__first_message_receival = False
        logdebug(LOGGER, 'Batch sending: Handing %i messages over to the sender.', len(messages))
        self.__logcounter_received += len(messages)

    def __put_all_messages_into_queue_of_unsent_messages(self, messages):
        counter = 1
        for message in messages:
            logtrace(LOGGER, 'Adding message %i/%i to stack to be sent.', counter, len(messages))
            counter += 1
            self.__put_one_message_into_queue_of_unsent_messages(message)

    def __trigger_one_publish_action(self):
        self.__thread.add_event_publish_message()

    def __trigger_n_publish_actions(self, num_messages_to_publish):
        logdebug(LOGGER, 'Asking rabbit thread to publish %i messages...', num_messages_to_publish)
        to_be_sure = 10
        for i in xrange(num_messages_to_publish+to_be_sure):
            self.__thread.add_event_publish_message()


    ###############
    ### Getters ###
    ###############

    '''
    Returns a copy of the messages that were not confirmed during
    the rabbit thread's lifetime.
    Can only be called after the thread's join, as dict objects
    are not thread-safe.

    :raises: OperationNotAllowed: If the thread is still alive.
    :return: A list containing all unconfirmed messages.
    '''
    def __get_unconfirmed_messages_as_list(self):
        return self.__thread.get_unconfirmed_messages_as_list_copy()

    '''
    Returns a copy of the messages that were nacked during
    the rabbit thread's lifetime.
    Can only be called after the thread's join, as dict objects
    are not thread-safe.

    :raises: OperationNotAllowed: If the thread is still alive.
    :return: A list containing all nacked messages.
    '''
    def __get_nacked_messages_as_list(self):
        return self.__thread.get_nacked_messages_as_list()

    '''
    Returns a new list containing all unpublished messages.

    Note: As this method gets the content of a thread-safe queue,
    there is no harm in calling it during the rabbit thread's lifetime.
    However, we restrict its use to after its joining, because we
    only want to use it to "rescue" messages that have not been
    published once publishing definitely has ended.

    The method iterates over the Queue and retrieves objects until
    a Queue.Empty event occurs. As this event is not guaranteed
    to be true, the retrieval is then tried another time with
    a little timeout to make sure all elements are retrieved.

    This complicated way of making a copy is not necessary
    if the method is only called after the thread's join...

    :raises: OperationNotAllowed: If the thread is still alive.
    :return: A list containing all unpublished messages.
    '''
    def __get_unpublished_messages_as_list(self):
        newlist = []
        counter = 0
        to_be_safe = 10
        max_iterations = self.__unpublished_messages_queue.qsize() + to_be_safe
        while True:
            counter+=1
            if counter == max_iterations:
                break
            try:
                self.__get_msg_from_queue_and_store_first_try(
                    newlist,
                    self.__unpublished_messages_queue)
            except Queue.Empty:
                try:
                    self.__get_a_msg_from_queue_and_store_second_try(
                        newlist,
                        self.__unpublished_messages_queue)
                except Queue.Empty:
                    break
        return newlist

    '''Put a message from the Queue to the list, without waiting.'''
    def __get_msg_from_queue_and_store_first_try(self, alist, queue):
        msg_incl_routing_key = queue.get(block=False)
        alist.append(msg_incl_routing_key)

    '''Put a message from the Queue to the list, with waiting.'''
    def __get_a_msg_from_queue_and_store_second_try(self, alist, queue):
        wait_seconds = 0.5
        msg_incl_routing_key = queue.get(block=True, timeout=wait_seconds)
        alist.append(msg_incl_routing_key)
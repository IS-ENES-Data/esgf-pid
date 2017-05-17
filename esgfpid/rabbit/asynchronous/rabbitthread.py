import Queue
import threading
import pika
import time
import copy
import datetime
import logging
import esgfpid.utils
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from .thread_returnhandler import UnacceptedMessagesHandler
from .thread_statemachine import StateMachine
from .thread_builder import ConnectionBuilder
from .thread_feeder import RabbitFeeder
from .thread_shutter import ShutDowner
from .thread_confirmer import Confirmer
from .exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
Statemachine is WRITTEN by thread, READ by main.
All other thread_ modules are only WRITTEN by thread.

Main can only influence the thread via run() and via add_event(), and by passing
messages into the thread-safe queue.

How can main get information from the thread?
'''


'''
The class RabbitThread is the parallel thread that is
responsible for the asynchronous communication with the
RabbitMQ server.

It has several submodules that do the actual work - connecting
to RabbitMQ, publishing messages, receiving confirms, handling
connection problems, ... (Those submodules are named "thread_...").

RabbitThread implements its own thread, which, after connecting to
RabbitMQ, basically stays in a blocking loop (ioloop) waiting
for events.

These events can come from the RabbitMQ (via defined callback
methods) or from the main thread. Only the connector object
(AsynchronousRabbitConnector) has a reference to the thread and
passes events to it.

The RabbitThread provides some methods to be called by the main
thread, i.e. by the AsynchronousRabbitConnector, which use pika's
SelectConnection.add_timeout() to pass the event to the thread's
blocking loop.

Only the methods
 * run()
 * add_event_publish_message()
 * add_event_gently_finish()
 * add_event_force_finish()
should be called from the AsynchronousRabbitConnector.

All other methods are thread-UNsafe and to be used by the
RabbitThread's submodules only. They are only public to
enable the submodules to communicate among each other
without needed references to each other.

'''
class RabbitThread(threading.Thread):

    def __init__(self, statemachine, queue, facade, node_manager):
        threading.Thread.__init__(self)

        '''
        The rabbit module used by the library to publish modules
        to this thread. In some situations the thread itself needs
        to be able to publish messages the same way.
        Type: esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector.
        '''
        self.__facade = facade

        '''
        State-machine.
        Shared with the main thread!
        Not thread-safe.
        '''
        self.__statemachine = statemachine

        '''
        Synchronization event which makes sure the main thread
        waits for the rabbit thread to finish shutting down, without
        having to use time.sleep().
        Shared with the main thread!
        '''
        self.__gently_finish_ready = threading.Event()

        '''
        Synchronization event which makes sure the main thread
        waits for the rabbit thread to be ready for events before
        sending events, without having to use time.sleep().
        Shared with the main thread!
        '''
        self.__connection_is_set = threading.Event()
        
        '''
        Thread-safe Queue that will contain the unpublished messages.
        The main thread will put messages into it, and the rabbit thread 
        will retrieve and publish them.
        Shared with the main thread!
        '''
        self.__unpublished_messages_queue = queue

        # These are only used by the rabbit thread:
        '''
        If the messages should not be published to the exchange that
        was passed from the publisher in config, but to a fallback 
        solution, this will be set:
        '''
        self.__fallback_exchange = None
        
        '''
        An object of type "pika.SelectConnection".
        It contains the connection to the RabbitMQ.
        It provides a channel to do the publications and
        an ioloop that blocks and waits for events.

        The connection is initialized by the builder submodule.
        Not used by feeder.
        '''
        self._connection = None

        '''
        An object of type "pika.channel.Channel".

        Used by the feeder for basic_publish, for logging
        its channel number, and for checking whether it
        exists (before publishing).
        '''
        self._channel = None

        # Submodules that do the actual work:
        self.__nodemanager = node_manager
        self.__confirmer = Confirmer()
        self.__returnhandler = UnacceptedMessagesHandler(self)
        self.__feeder = RabbitFeeder(self, self.__statemachine, self.__nodemanager)
        self.__shutter = ShutDowner(self, self.__statemachine)

        '''
        Needed to trigger the connection in run()
        '''
        self.__builder = ConnectionBuilder(self, self.__statemachine, self.__confirmer, self.__returnhandler, self.__shutter, node_manager)


        '''
        This determines how many seconds the client will wait until a message
        is published to the server, after having handed it over to the "connection".
        Obviously, zero is highly recommended.

        The fact that it is possible to increase it requires some precautions.
        If messages can be scheduled to be sent to server, but not actually sent
        yet makes it difficult to loop over the Queue of unsent messages, using
        "while" and "Queue.Empty" to break it.
        (It will loop for too long and then have to call the publish method many
        many times with no more messages to publish.)

        If you ever decide to while-loop and break on Queue.Empty, just make sure
        the publish interval is zero.
        '''
        self.__PUBLISH_INTERVAL_SECONDS = 0

        # Error codes
        self.ERROR_CODE_CONNECTION_CLOSED_BY_USER=999
        self.ERROR_TEXT_CONNECTION_FORCE_CLOSED='(forced finish)'
        self.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN='(not reopen)'
        self.ERROR_TEXT_CONNECTION_PERMANENT_ERROR='(permanent error)'

    #
    # Methods called from the main thread
    #

    '''
    The run() method to implement threading.Thread.

    run() is called once in the beginning.
    Everything that is called inside run() is executed by
    the rabbit thread.
    '''
    def run(self):
        logdebug(LOGGER, 'Starting thread...')
        self.__builder.first_connection()

    '''
    At close down because of permanent errors.
    We need to make sure the main thread does not wait for 
    any event.
    '''
    def unblock_events(self):
        self.__gently_finish_ready.set()
        self.__connection_is_set.set()

    def add_event_publish_message(self):
        logdebug(LOGGER, 'Asking rabbit thread to publish a message...')
        self.__add_event(self.__feeder.publish_message)
        self.__add_event(self.__feeder.publish_message) # Send two...
        logdebug(LOGGER, '(Trigger sent.)')

    def add_event_force_finish(self):
        logdebug(LOGGER, 'Asking rabbit thread to finish quickly...')
        self.__add_event(self.__shutter.force_finish)

    def add_event_gently_finish(self):
        logdebug(LOGGER, 'Asking rabbit thread to finish...')
        self.__add_event(self.__shutter.finish_gently)
        self.__wait_for_thread_to_finish_gently()
        # This waiting is necessary, because after finishing gently
        # the main thread needs to join the rabbit thread, which
        # only makes sense if finishing gently is done.
        # If we separated the finish_gently command and the join()
        # command, the main thread could do other stuff in the mean-
        # time, but we'd have to trust the user to call both, to
        # avoid missing the join.

    '''
    Allows the main thread to retrieve all unconfirmed messages.
    As the method is not thread safe, it can be called only
    after the thread is joined.

    It makes a new copy, so the returned list may be modified
    without causing any harm.

    Note: If the method needs to be called inside the running
    thread, it can be called on the confirmer module directly,
    not using this facade.

    :raises: OperationNotAllowed: If the thread is still alive.
    :return: A list containing all unconfirmed messages.
    '''
    def get_unconfirmed_messages_as_list_copy(self):
        if not self.is_alive():
            return self.__confirmer.get_unconfirmed_messages_as_list_copy()
        else:
            raise OperationNotAllowed('thread has not finished','retrieving unconfirmed messages')

    '''
    Allows the main thread to retrieve all nacked messages.
    As the method is not thread safe, it can be called only
    after the thread is joined.

    It makes a new copy, so the returned list may be modified
    without causing any harm.

    Note: If the method needs to be called inside the running
    thread, it can be called on the confirmer module directly,
    not using this facade.

    :raises: OperationNotAllowed: If the thread is still alive.
    :return: A list containing all nacked messages.
    '''
    def get_nacked_messages_as_list(self):
        if not self.is_alive():
            return self.__confirmer.get_copy_of_nacked()
        else:
            raise OperationNotAllowed('thread has not finished','retrieving nacked messages')
 

    #
    # Helpers
    #

    '''
    This is how to add an event to the rabbit thread
    from the main thread. 
    '''
    def __add_event(self, event):
        if self._connection is not None:
            self._connection.add_timeout(self.__PUBLISH_INTERVAL_SECONDS, event)
        else:
            # If the main thread wants to add an event so quickly after starting the
            # thread that not even the connection object is listening for events yet,
            # we need to force it to wait.
            # Event listening is the first thing that happens when a thread is started,
            # but e.g. for shopping carts, the main thread just sends one message and
            # then wants to close again.
            # In that case, the thread cannot even receive the close event, as it is
            # not started yet.
            logdebug(LOGGER, 'Main thread wants to add event to thread that is not ready to receive events yet. Blocking and waiting.')
            self.__wait_for_thread_to_accept_events()
            logdebug(LOGGER, 'Thread declared itself ready to receive events.')
            self._connection.add_timeout(self.__PUBLISH_INTERVAL_SECONDS, event)
            logerror(LOGGER, 'Added event after having waited for thread to open.')



    '''
    Force the main thread to wait for the rabbit thread 
    while it tries to finish gently.
    Called from "add_event_gently_finish()".
    Executed by the main thread.
    BLOCKS THE MAIN THREAD (this is its purpose!)
    '''
    def __wait_for_thread_to_finish_gently(self):
        # This is executed by the main thread and thus should be placed in the
        # main thread's method, but it is so important not to forget to wait,
        # that it is better to have it boung to the thread's method "add_event_gently_finish()"
        logdebug(LOGGER, 'Now waiting for gentle close-down of RabbitMQ connection...')
        self.__gently_finish_ready.wait()
        logdebug(LOGGER, 'Finished waiting for gentle close-down of RabbitMQ connection.')

    def __wait_for_thread_to_accept_events(self):
        logdebug(LOGGER, 'Now waiting for the connection before I can even close RabbitMQ connection...')
        self.__connection_is_set.wait()

    def tell_publisher_to_stop_waiting_for_thread_to_accept_events(self):
        self.__connection_is_set.set()
        logdebug(LOGGER, 'Finished waiting for thread to start.')
 
    #
    # Methods called from inside the thread
    # These are to be called only by the submodules!
    # As they are not necessarily thread-safe, and
    # may change the functioning of the module!
    #

    '''
    Fire the event (threading.Event)!

    Called by shutter, to tell when the main thread should
    not wait any longer for the gently-finish command.
    (Previously, this was done by using time.sleep() in
    the main thread, but that causes problems, as it messes
    up the proper and punctual receival of RabbitMQ confirms.
    '''
    def tell_publisher_to_stop_waiting_for_gentle_finish(self):
        self.__gently_finish_ready.set()

    '''
    Called by confirmer, once the connection is there, to see if any
    messages have arrived in the mean time to be published.

    Called by feeder, for logging how many messages are left after
    a publish action.

    Called by shutter, to check if all messages were published.
    '''
    def get_num_unpublished(self):
        return self.__unpublished_messages_queue.qsize()

    ''' Called by shutter, to check if all messages were confirmed. '''
    def get_num_unconfirmed(self):
        return self.__confirmer.get_num_unconfirmed()

    ''' Called by feeder, to publish a message. May raise Queue.Empty. '''
    def get_message_from_unpublished_stack(self, seconds):
        return self.__unpublished_messages_queue.get(block=True, timeout=seconds) # can raise Queue.Empty

    ''' Called by feeder, to put a message back that was not successfully published. '''
    def put_one_message_into_queue_of_unsent_messages(self, message):
        return self.__unpublished_messages_queue.put(message, block=False)

    ''' Called by builder, to republish messages after a reconnection. '''
    def send_many_messages(self, messages):
        return self.__facade.send_many_messages_to_queue(messages)

    '''Called by returnhandle, to republish a message that was not accepted.'''
    def send_a_message(self, message):
        return self.__facade.send_message_to_queue(message)

    '''Called by feeder, to notify confirmer about which message it needs to get confirmed. '''
    def put_to_unconfirmed_delivery_tags(self, delivery_tag):
        return self.__confirmer.put_to_unconfirmed_delivery_tags(delivery_tag)

    '''Called by feeder, to notify confirmer about which message it needs to get confirmed. '''
    def put_to_unconfirmed_messages_dict(self, delivery_tag, message):
        return self.__confirmer.put_to_unconfirmed_messages_dict(delivery_tag, message)

    ''' Called by builder, to prepare message republication after reconnect/channel reopen. '''
    def reset_unconfirmed_messages_and_delivery_tags(self):
        return self.__confirmer.reset_unconfirmed_messages_and_delivery_tags()

    ''' Called by builder, to prepare message republication after reconnect/channel reopen. '''
    def get_unconfirmed_messages_as_list_copy_during_lifetime(self):
        return self.__confirmer.get_unconfirmed_messages_as_list_copy()

    ''' Called by builder, to prepare message republication after reconnect/channel reopen. '''
    def reset_delivery_number(self):
        return self.__feeder.reset_delivery_number()

    ''' Called by feeder. Called by builder, only for logging. '''
    def get_exchange_name(self):
        if self.__fallback_exchange is not None:
            return self.__fallback_exchange
        else:
            return self.__nodemanager.get_exchange_name()

    ''' Called by builder, in case the old exchange caused an error.'''
    def change_exchange_name(self, new_name):
        self.__fallback_exchange = new_name

    ''' Called by builder, in case the old exchange caused an error.'''
    def reset_exchange_name(self):
        self.__fallback_exchange = None

    ''' Called by shutter, in case a connectio is already closing/closed... '''
    def make_permanently_closed_by_user(self):
        return self.__builder.make_permanently_closed_by_user()

    ''' Called by builder any time a new ioloop starts to listen.'''
    def continue_gently_closing_if_applicable(self):
        return self.__shutter.continue_gently_closing_if_applicable()

    ''' Called by feeder to add a word (trusted/untrusted rabbit?)
    to the routing key. '''
    def get_open_word_for_routing_key(self):
        return self.__nodemanager.get_open_word_for_routing_key()
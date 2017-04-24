import logging
import pika
import Queue
from .. import rabbitutils
import esgfpid.defaults as defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
The RabbitFeeder is responsible for publishing messages to RabbitMQ.

It is very simple. Basically the only method it exposes
(except for some simple getter/setter which is rarely ever used)
is publish_message(), which is called from the main thread.

'''
class RabbitFeeder(object):

    def __init__(self, thread, statemachine, nodemanager):
        self.thread = thread

        '''
        Read-only.
        Before publishing a message, we check the state, and we log 
        the state. '''
        self.statemachine = statemachine

        self.nodemanager = nodemanager

        '''
        The deliver_number is important. It defines the number of the message
        that is used to identify it between this client and the RabbitMQ
        server (e.g. so the correct messages are deleted upon confirmation).

        It makes sure that rabbit server and this client talk about the
        same message.
        NEVER EVER INCREMENT OR OTHERWISE MODIFY THIS!

        From the RabbitMQ docs:
        "The delivery tag is valid only within the channel from which
        the message was received. I.e. a client MUST NOT receive a
        message on one channel and then acknowledge it on another."
        Source: https://www.rabbitmq.com/amqp-0-9-1-reference.html '''
        self.__delivery_number = 1

        # Logging
        self.__first_publication_trigger = True
        self.__logcounter_success = 0 # counts successful publishes!
        self.__logcounter_trigger = 0 # counts triggers!
        self.__LOGFREQUENCY = 10
        self.__have_not_warned_about_connection_fail_yet = True
        self.__have_not_warned_about_force_close_yet = True

    '''
    Triggers the publication of one message to RabbitMQ, if the
    state machine currently allows this.

    The message is fetched from the Queue of unpublished messages.

    So far, whenever the library wants to publish messages, it
    fires as many of these "publish_message" events as messages
    were published (and some extra, to be sure).
    If some of these triggers cannot be acted upon, as the module
    is not in a state where it is allowed to publish, the triggers
    should be fired as soon as the module is in available state
    again.

    # TODO: Find better way to make sure enough publish events are fired.
    Are we sure there is not ever a way to have some messages
    in the unpublished Queue that could be sent, but aren't, because
    no event was fired for them? For example, if an exception occurs
    during publish, and the message was put back - will there ever
    be an event to trigger its publication? I don't think so.

    Interim solution (hack):
    (a) At the moment, for every message that the publisher hands 
        over, I fire two events (rabbitthread).
    (b) During the close-down algorithm, if there is unpublished
        messages, I fire publish events, to make sure they are
        published (thread_shutter).

    '''
    def publish_message(self):
        try:
            return self.__publish_message()
        except Exception as e:
            logwarn(LOGGER, 'Error in feeder.publish_message(): %s: %s', e.__class__.__name__, e.message)
            raise e

    def __publish_message(self):
        self.__logcounter_trigger += 1

        if self.statemachine.is_NOT_STARTED_YET() or self.statemachine.is_WAITING_TO_BE_AVAILABLE():
            log_every_x_times(LOGGER, self.__logcounter_trigger, self.__LOGFREQUENCY, 'Received early trigger for feeding the rabbit (trigger %i).', self.__logcounter_trigger)
            self.__log_why_cannot_feed_the_rabbit_now()

        elif self.statemachine.is_AVAILABLE() or self.statemachine.is_AVAILABLE_BUT_WANTS_TO_STOP():
            log_every_x_times(LOGGER, self.__logcounter_trigger, self.__LOGFREQUENCY, 'Received trigger for publishing message to RabbitMQ (trigger %i).', self.__logcounter_trigger)
            self.__log_publication_trigger()
            self.__publish_message_to_channel()

        elif self.statemachine.is_PERMANENTLY_UNAVAILABLE() or self.statemachine.is_FORCE_FINISHED():
            log_every_x_times(LOGGER, self.__logcounter_trigger, self.__LOGFREQUENCY, 'Received late trigger for feeding the rabbit (trigger %i).', self.__logcounter_trigger)
            self.__log_why_cannot_feed_the_rabbit_now()

    ''' This method only logs. '''
    def __log_publication_trigger(self):
        if self.__first_publication_trigger:
            logdebug(LOGGER, 'Received first trigger for publishing message to RabbitMQ.')
            self.__first_publication_trigger = False
        logtrace(LOGGER, 'Received trigger for publishing message to RabbitMQ, and module is ready to accept it.')

    ''' This method only logs, depending on the state machine's state.'''
    def __log_why_cannot_feed_the_rabbit_now(self):
        log_every_x_times(LOGGER, self.__logcounter_trigger, self.__LOGFREQUENCY, 'Cannot publish message to RabbitMQ (trigger no. %i).', self.__logcounter_trigger)
        if self.statemachine.is_WAITING_TO_BE_AVAILABLE():
            logdebug(LOGGER, 'Cannot publish message to RabbitMQ yet, as the connection is not ready.')
        elif self.statemachine.is_NOT_STARTED_YET():
            logerror(LOGGER, 'Cannot publish message to RabbitMQ, as the thread is not running yet.')
        elif self.statemachine.is_PERMANENTLY_UNAVAILABLE() or self.statemachine.is_FORCE_FINISHED():
            if self.statemachine.detail_could_not_connect:
                logtrace(LOGGER, 'Could not publish message to RabbitMQ, as the connection failed.')
                if self.__have_not_warned_about_connection_fail_yet:
                    logwarn(LOGGER, 'Could not publish message(s) to RabbitMQ. The connection failed definitively.')
                    self.__have_not_warned_about_connection_fail_yet = False
            elif self.statemachine.get_detail_closed_by_publisher():
                logtrace(LOGGER, 'Cannot publish message to RabbitMQ, as the connection was closed by the user.')
                if self.__have_not_warned_about_force_close_yet:
                    logwarn(LOGGER, 'Could not publish message(s) to RabbitMQ. The sender was closed by the user.')
                    self.__have_not_warned_about_force_close_yet = False
        else:
            if self.thread._channel is None:
                logerror(LOGGER, 'Very unexpected. Could not publish message(s) to RabbitMQ. There is no channel.')

    '''
    Retrieves a message from stack and tries to publish it
    to RabbitMQ.
    In case of failure, it is put back. In case of success,
    it is handed on to the confirm module that is responsible
    for waiting for RabbitMQ's confirmation.

    Note: The publish may cause an error if the Channel was closed.
    A closed Channel should be handled in the on_channel_close()
    callback, but we catch it here in case the clean up was not quick enough.
    '''
    def __publish_message_to_channel(self):

        # Find a message to publish.
        # If no messages left, well, nothing to publish!
        try:
            message = self.__get_message_from_stack()
        except Queue.Empty as e:
            logtrace(LOGGER, 'Queue empty. No more messages to be published.')
            return

        # Now try to publish it.
        # If anything goes wrong, you need to put it back to
        # the stack of unpublished messages!
        try:
            success = self.__try_publishing_otherwise_put_back_to_stack(message)
            if success:
                self.__postparations_after_successful_feeding(message)

        # Treat various errors that may occur during publishing:
        except pika.exceptions.ChannelClosed as e:
            logwarn(LOGGER, 'Cannot publish message %i to RabbitMQ because the Channel is closed (%s)', self.__delivery_number+1, e.message)

        except AttributeError as e:
            if self.thread._channel is None:
                logwarn(LOGGER, 'Cannot publish message %i to RabbitMQ because there is no channel.', self.__delivery_number+1)
            else:
                logwarn(LOGGER, 'Cannot publish message %i to RabbitMQ (unexpected error %s:%s)', self.__delivery_number+1, e.__class__.__name__, e.message)

        except AssertionError as e:
            logwarn(LOGGER, 'Cannot publish message to RabbitMQ %i because of AssertionError: "%s"', self.__delivery_number+1,e)
            if e.message == 'A non-string value was supplied for self.exchange':
                exch = self.thread.get_exchange_name()
                logwarn(LOGGER, 'Exchange was "%s" (type %s)', exch, type(exch))


    '''
    Retrieve an unpublished message from stack.
    Note: May block for up to 2 seconds.

    :return: A message from the stack of unpublished messages.
    :raises: Queue.Empty.
    '''
    def __get_message_from_stack(self, seconds=0):
        message = self.thread.get_message_from_unpublished_stack(seconds)
        logtrace(LOGGER, 'Found message to be published. Now left in queue to be published: %i messages.', self.thread.get_num_unpublished())
        return message

    '''
    This tries to publish the message and puts it back into the
    Queue if it failed.

    :param message: Message to be sent.
    :raises: pika.exceptions.ChannelClosed, if the Channel is closed.
    '''
    def __try_publishing_otherwise_put_back_to_stack(self, message):
        try:
            # Getting message info:
            properties = self.nodemanager.get_properties_for_message_publications()
            routing_key, msg_string = rabbitutils.get_routing_key_and_string_message_from_message_if_possible(message)
            routing_key = routing_key+'.'+self.thread.get_open_word_for_routing_key()
            
            # Logging
            logtrace(LOGGER, 'Publishing message %i (key %s) (body %s)...', self.__delivery_number+1, routing_key, msg_string) # +1 because it will be incremented after the publish.
            log_every_x_times(LOGGER, self.__logcounter_trigger, self.__LOGFREQUENCY, 'Trying actual publish... (trigger no. %i).', self.__logcounter_trigger)
            logtrace(LOGGER, '(Publish to channel no. %i).', self.thread._channel.channel_number)

            # Actual publish to exchange
            self.thread._channel.basic_publish(
                exchange=self.thread.get_exchange_name(),
                routing_key=routing_key,
                body=msg_string,
                properties=properties,
                mandatory=defaults.RABBIT_MANDATORY_DELIVERY
            )
            return True

        # If anything went wrong, put it back into the stack of
        # unpublished messages before re-raising the exception
        # for further handling:
        except Exception as e:
            success = False
            logwarn(LOGGER, 'Message was not published. Putting back to queue. Reason: %s: "%s"',e.__class__.__name__, e.message)
            self.thread.put_one_message_into_queue_of_unsent_messages(message)
            logtrace(LOGGER, 'Now (after putting back) left in queue to be published: %i messages.', self.thread.get_num_unpublished())
            raise e

    '''
    If a publish was successful, pass it to the confirmer module
    and in increment delivery_number for the next message.
    '''
    def __postparations_after_successful_feeding(self, msg):

        # Pass the successfully published message and its delivery_number
        # to the confirmer module, to wait for its confirmation.
        # Increase the delivery number for the next message.
        self.thread.put_to_unconfirmed_delivery_tags(self.__delivery_number)
        self.thread.put_to_unconfirmed_messages_dict(self.__delivery_number, msg)
        self.__delivery_number += 1

        # Logging
        self.__logcounter_success += 1
        log_every_x_times(LOGGER, self.__logcounter_success, self.__LOGFREQUENCY, 'Actual publish to channel done (trigger no. %i, publish no. %i).', self.__logcounter_trigger, self.__logcounter_success)
        logtrace(LOGGER, 'Publishing messages %i to RabbitMQ... done.', self.__delivery_number-1)
        if (self.__delivery_number-1 == 1):
            loginfo(LOGGER, 'First message published to RabbitMQ.')
        logdebug(LOGGER, 'Message published (no. %i)', self.__delivery_number-1)

    '''
    Reset the delivery_number for the messages.
    This must be called on a reconnection / channel reopen!
    And may not be called during any other situation!

    The number is not sent along to the RabbitMQ server, but
    the server keeps track of the delivery number
    separately on its side.

    That's why it is important to make sure it is incremented
    and reset exactly the same way (incremented at each successfully
    published message, and reset to one at channel reopen).

    (called by the builder during reconnection / channel reopen).
    '''
    def reset_delivery_number(self):
        self.__delivery_number = 1

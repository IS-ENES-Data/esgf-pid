import logging
import copy
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from .exceptions import UnknownServerResponse

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
=========
Confirmer
=========

The confirmer is responsible for handling the confirms sent by RabbitMQ.
This is a quite simple module, it only reacts and does not trigger any
other actions itself.

It has a stack of unconfirmed messages (which is filled by the feeder,
it puts every message it has successfully published into that stack).

For each confirm, it must check what kind of confirm it is (ack/nack, single/multiple),
and act accordingly. Confirmed messages  and their delivery numbers must be deleted
from the stack. Unconfirmed messages remain. Nacked messages are stored in an
extra stack.

The unconfirmed messages can be retrieved from the confirmer to be republished.

API:
 * on_delivery_confirmation() is called by RabbitMQ.
 * reset_unconfirmed_messages_and_delivery_tags() called by builder, during reconnection
 * get_unconfirmed_messages_as_list_copy() called by builder, during reconnection
 * put_to_unconfirmed_delivery_tags() called by feeder, to fill the stack
 * put_to_unconfirmed_messages_dict() called by feeder, to fill the stack

'''

class Confirmer(object):

    def __init__(self):

        # Logging:
        self.__first_confirm_receival = True
        self.__logcounter = 1
        self.__LOGFREQUENCY = 10

        # Stacks of unconfirmed/nacked messages:
        self.__unconfirmed_delivery_tags = [] # only accessed internally
        self.__unconfirmed_messages_dict = {} # dict, because I need to retrieve them by delivery tag (on ack/nack)
        self.__nacked_messages = []           # only accessed internally, and from outside after thread is dead

    '''
    Callback, called by RabbitMQ.
    '''
    def on_delivery_confirmation(self, method_frame):
        deliv_tag, confirmation_type, multiple = self.__get_confirm_info(method_frame)
        log_every_x_times(LOGGER, self.__logcounter, self.__LOGFREQUENCY, 'Received a confirm (%s)', confirmation_type)
        self.__logcounter += 1

        if confirmation_type == 'ack':
            logtrace(LOGGER, 'Received "ACK" from messaging service.')
            self.__react_on_ack(deliv_tag, multiple)
        elif confirmation_type == 'nack':
            logtrace(LOGGER, 'Received "NACK" from messaging service.')
            self.__react_on_nack(deliv_tag, multiple)
        else:
            msg = 'Received asynchronous response of unknown type from messaging service.'
            logwarn(LOGGER, msg)
            raise UnknownServerResponse(msg+':'+str(method_frame))
            # This should never happen, unless if I parse the server's response wrongly.

    def __react_on_ack(self, deliv_tag, multiple):
        if self.__first_confirm_receival:
            self.__first_confirm_receival = False
            loginfo(LOGGER, 'Received first message confirmation from RabbitMQ.')

        if multiple:
            logtrace(LOGGER, 'Received "ACK" for multiple messages from messaging service.')
            self.__react_on_multiple_delivery_ack(deliv_tag)
        else:
            logtrace(LOGGER, 'Received "ACK" for single message from messaging service.')
            self.__react_on_single_delivery_ack(deliv_tag)


    def __react_on_nack(self, deliv_tag, multiple):
        if multiple:
            logwarn(LOGGER, 'Received "NACK" for delivery tag: %i and below.', deliv_tag)
            self.__nack_delivery_tag_and_message_several(deliv_tag)
        else:
            logwarn(LOGGER, 'Received "NACK" for delivery tag: %i.', deliv_tag)
            self.__nack_delivery_tag_and_message_single(deliv_tag)

    def __nack_delivery_tag_and_message_single(self, deliv_tag):
        msg = self.__unconfirmed_messages_dict.pop(str(deliv_tag))
        self.__nacked_messages.append(msg)
        self.__unconfirmed_delivery_tags.remove(deliv_tag)

    def __nack_delivery_tag_and_message_several(self, deliv_tag):
        for candidate_deliv_tag in copy.copy(self.__unconfirmed_delivery_tags):
            if candidate_deliv_tag <= deliv_tag:
                self.__nack_delivery_tag_and_message_single(candidate_deliv_tag)

    def __get_confirm_info(self, method_frame):
        try:
            deliv_tag = method_frame.method.delivery_tag # integer
            confirmation_type = method_frame.method.NAME.split('.')[1].lower() # "ack" or "nack" or ...
            multiple = method_frame.method.multiple # Boolean
            return deliv_tag, confirmation_type, multiple
        except AttributeError as e:
            raise UnknownServerResponse(str(method_frame)+' - '+e.message)
        except IndexError as e:
            raise UnknownServerResponse(str(method_frame)+' - '+e.message)

    def __react_on_single_delivery_ack(self, deliv_tag):
        self.__remove_delivery_tag_and_message_single(deliv_tag)
        logdebug(LOGGER, 'Received ack for delivery tag %i. Waiting for %i confirms.', deliv_tag, len(self.__unconfirmed_delivery_tags))
        logtrace(LOGGER, 'Received ack for delivery tag %i.', deliv_tag)
        logtrace(LOGGER, 'Now left in queue to be confirmed: %i messages.', len(self.__unconfirmed_delivery_tags))

    def __react_on_multiple_delivery_ack(self, deliv_tag):
        self.__remove_delivery_tag_and_message_several(deliv_tag)
        logdebug(LOGGER, 'Received ack for delivery tag %i and all below. Waiting for %i confirms.', deliv_tag, len(self.__unconfirmed_delivery_tags))
        logtrace(LOGGER, 'Received ack for delivery tag %i and all below.', deliv_tag)
        logtrace(LOGGER, 'Now left in queue to be confirmed: %i messages.', len(self.__unconfirmed_delivery_tags))

    def __remove_delivery_tag_and_message_single(self, deliv_tag):
        try:
            self.__unconfirmed_delivery_tags.remove(deliv_tag)
            ms = self.__unconfirmed_messages_dict.pop(str(deliv_tag))
            logtrace(LOGGER, 'Received ack for message %s.', ms)
        except ValueError as e:
            logdebug(LOGGER, 'Could not remove %i from unconfirmed.', deliv_tag)

    def __remove_delivery_tag_and_message_several(self, deliv_tag):
        for candidate_deliv_tag in copy.copy(self.__unconfirmed_delivery_tags):
            if candidate_deliv_tag <= deliv_tag:
                self.__remove_delivery_tag_and_message_single(candidate_deliv_tag)


    ''' Called by unit test.'''
    def get_num_unconfirmed(self):
        return len(self.__unconfirmed_messages_dict)

    ''' Called by unit test.'''
    def get_copy_of_unconfirmed_tags(self):
        return self.__unconfirmed_delivery_tags[:]

    '''
    Called by the main thread, for rescuing, after joining.
    And by unit test.
    '''
    def get_copy_of_nacked(self):
        return self.__nacked_messages[:]

    '''
    Called by feeder, to let the confirmer know which had been sent.
    '''
    def put_to_unconfirmed_delivery_tags(self, delivery_tag):
        logtrace(LOGGER, 'Adding delivery tag %i to unconfirmed.', delivery_tag)
        self.__unconfirmed_delivery_tags.append(delivery_tag)

    '''
    Called by feeder, to let the confirmer know which had been sent.
    '''
    def put_to_unconfirmed_messages_dict(self, delivery_tag, msg):
        logtrace(LOGGER, 'Adding message with delivery tag %i to unconfirmed: %s', delivery_tag, msg)
        self.__unconfirmed_messages_dict[str(delivery_tag)] = msg

    '''
    This resets which messages had not be confirmed yet.
    
    IMPORTANT:
    Before doing this, retrieve the unconfirmed messages
    and republish them! Otherwise, they will be lost!
    (We do not do this here, as we have no reference to
    the builder module, to avoid circular references).

    Called by builder, during reconnection.
    After a reconnection, no more confirms can be received,
    so the messages that are unconfirmed now cannot be
    confirmed any more.

    Any new confirms that arrive after a reconnection will
    carry new delivery tags (as they start over at one),
    so we need to reset the old ones, otherwise we'll take
    the wrong messages as confirmed.

    From RabbitMQ docs:
    "The server-assigned and channel-specific delivery tag. The delivery tag is valid only within the channel from which the message was received. [...] The server MUST NOT use a zero value for delivery tags."
    See: https://www.rabbitmq.com/amqp-0-9-1-reference.html

    '''
    def reset_unconfirmed_messages_and_delivery_tags(self):
        self.__unconfirmed_delivery_tags = []
        self.__unconfirmed_messages_dict = {}

    '''
    Called by builder, during reconnection,
    to rescue these unconfirmed messages, in order to
    republish them, as soon as a new connection is
    available.

    Also called by the main thread, for rescuing messages
    after joining.

    As dict objects are not thread-safe, better call
    this only after joining.
    '''
    def get_unconfirmed_messages_as_list_copy(self):
        newlist = []
        for deliv_tag,message in self.__unconfirmed_messages_dict.iteritems():
            newlist.append(message)
        return newlist

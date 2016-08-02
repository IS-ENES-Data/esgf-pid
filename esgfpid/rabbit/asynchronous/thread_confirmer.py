import logging
import copy
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from .exceptions import UnknownServerResponse

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class ConfirmReactor(object):

    def __init__(self):

        self.lc = 1
        self.li = 10

        self.__unconfirmed_delivery_tags = [] # only accessed internally
        self.__unconfirmed_messages_dict = {} # dict, because I need to retrieve them by delivery tag (on ack/nack)
        self.__nacked_messages = []           # only accessed internally, and from outside after thread is dead


    ###########################
    ### Receive and process ###
    ### delivery confirms   ###
    ###########################

    def on_delivery_confirmation(self, method_frame):
        deliv_tag, confirmation_type, multiple = self.__get_confirm_info(method_frame)
        log_every_x_times(LOGGER, self.lc, self.li, 'Confirm %s', confirmation_type)

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
        logtrace(LOGGER, 'Received ack for delivery tag %i. Waiting for %i confirms.', deliv_tag, len(self.__unconfirmed_delivery_tags))
        logtrace(LOGGER, 'Received ack for delivery tag %i.', deliv_tag)
        logtrace(LOGGER, 'Now left in queue to be confirmed: %i messages.', len(self.__unconfirmed_delivery_tags))

    def __react_on_multiple_delivery_ack(self, deliv_tag):
        self.__remove_delivery_tag_and_message_several(deliv_tag)
        logtrace(LOGGER, 'Received "ACK" for delivery tag %i and all below. Waiting for %i confirms.', deliv_tag, len(self.__unconfirmed_delivery_tags))
        logtrace(LOGGER, 'Received "ACK" for delivery tag %i and all below.', deliv_tag)
        logtrace(LOGGER, 'Now left in queue to be confirmed: %i messages.', len(self.__unconfirmed_delivery_tags))

    def __remove_delivery_tag_and_message_single(self, deliv_tag):
        try:
            self.__unconfirmed_delivery_tags.remove(deliv_tag)
            self.__unconfirmed_messages_dict.pop(str(deliv_tag))
        except ValueError as e:
            logdebug(LOGGER, 'Could not remove %i from unconfirmed.', deliv_tag)

    def __remove_delivery_tag_and_message_several(self, deliv_tag):
        for candidate_deliv_tag in copy.copy(self.__unconfirmed_delivery_tags):
            if candidate_deliv_tag <= deliv_tag:
                self.__remove_delivery_tag_and_message_single(candidate_deliv_tag)

    def get_num_unconfirmed(self):
        return len(self.__unconfirmed_messages_dict)

    def get_copy_of_unconfirmed_tags(self):
        return self.__unconfirmed_delivery_tags[:]

    def get_copy_of_nacked(self):
        return self.__nacked_messages[:]

    def put_to_unconfirmed_delivery_tags(self, delivery_tag):
        logtrace(LOGGER, 'Adding delivery tag %i to unconfirmed.', delivery_tag)
        self.__unconfirmed_delivery_tags.append(delivery_tag)

    def put_to_unconfirmed_messages_dict(self, delivery_tag, msg):
        logtrace(LOGGER, 'Adding message with delivery tag %i to unconfirmed.', delivery_tag)
        self.__unconfirmed_messages_dict[str(delivery_tag)] = msg

    def reset_delivery_tags(self):
        # IMPORTANT:
        # Before doing this, retrieve the unconfirmed messages
        # and republish them via the acceptor module!
        # Currently done in the builder's "reconnect()" method.
        # Otherwise, they will be los.
        # Note: We do not do this here, as we have no reference
        # to builder or confirmer (avoid circular references).
        self.__unconfirmed_delivery_tags = []
        self.__unconfirmed_messages_dict = {}

    def get_unconfirmed_messages_as_list_copy(self):
        newlist = []
        for deliv_tag,message in self.__unconfirmed_messages_dict.iteritems():
            newlist.append(message)
        return newlist



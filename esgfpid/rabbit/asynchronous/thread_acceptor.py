import logging
import pika.exceptions
import json
import esgfpid.assistant.messages as messages
import esgfpid.defaults as defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from .exceptions import OperationNotAllowed

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class PublicationReceiver(object):

    def __init__(self, thread, statemachine, feeder):

        # Communication with objects:
        self.thread = thread
        self.statemachine = statemachine
        self.feeder = feeder

        # Own private attributes
        self.__first_message_sending = True
        self.__have_not_warned_about_unready_connection_yet = True
        self.__have_not_warned_about_connection_fail_yet = True
        self.__have_not_warned_about_force_close_yet = True
        self.lc = 1
        self.li = 10
        
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

    def __resend_an_unroutable_message(self, message):
        self.__send_a_message(message)

    def send_a_message(self, message):
        if self.statemachine.is_available_but_wants_to_stop():
            errormsg = 'Accepting no more messages'
            logwarn(LOGGER, errormsg+' (dropping %s).', message)
            raise OperationNotAllowed(errormsg)            
        else:
            self.__send_a_message(message)

    def __send_a_message(self, message):
        if self.__first_message_sending:
            logdebug(LOGGER, 'First message handed over to sender module...')
            self.__first_message_sending = False
        logtrace(LOGGER, 'Handing 1 message over to the sender (%s)', message)
        log_every_x_times(LOGGER, self.lc, self.li, 'Publisher handed me message')
        self.lc += 1
        self.__put_message_into_queue_of_unsent_messages(message)
        self.__trigger_publishing_one_message_if_ok()

    def send_many_messages(self, messages):
        if self.statemachine.is_available_but_wants_to_stop():
            errormsg = 'Accepting no more messages'
            logwarn(LOGGER, errormsg+' (dropping %i messages).', len(messages))
            raise OperationNotAllowed(errormsg)            
        else:
            logdebug(LOGGER, 'Batch sending: Handing %i messages over to the sender.', len(messages))
            self.__put_all_messages_into_queue_of_unsent_messages(messages)
            self.trigger_publishing_n_messages_if_ok(len(messages))

    def __put_all_messages_into_queue_of_unsent_messages(self, messages):
        counter = 0
        for message in messages:
            logtrace(LOGGER, 'Adding message %i/%i to stack to be sent.', counter, len(messages))
            counter += 1
            self.__put_message_into_queue_of_unsent_messages(message)
            
    def __put_message_into_queue_of_unsent_messages(self, message):
        self.feeder.put_message_into_queue_of_unsent_messages(message)

    def __trigger_publishing_one_message_if_ok(self):
        if self.statemachine.is_available_for_server_communication(): # triggering needs the connection, so we need to check here.
            self.__trigger_one_publish_action()
        else:
            self.__inform_why_cannot_trigger()

    def trigger_publishing_n_messages_if_ok(self, num_messages_to_publish):
        if self.statemachine.is_available_for_server_communication(): # triggering needs the connection, so we need to check here.
            self.__trigger_publishing_n_messages(num_messages_to_publish)
        else:
            self.__inform_why_cannot_trigger()

    def __trigger_publishing_n_messages(self, num_messages_to_publish):
        logdebug(LOGGER, 'Start sending %i messages from stack!', num_messages_to_publish)
        num_published = 0
        to_be_sure = 10
        for i in xrange(num_messages_to_publish+to_be_sure):
            self.__trigger_one_publish_action()

    def __inform_why_cannot_trigger(self):
        msg = 'Could not publish message'
        if self.statemachine.is_waiting_to_be_available():
            logtrace(LOGGER, msg+' yet, as the connection is not ready.')
            if self.__have_not_warned_about_unready_connection_yet:
                logwarn(LOGGER, msg+'. The connection is not ready.')
                self.__have_not_warned_about_unready_connection_yet = False

        elif self.statemachine.is_not_started_yet():
            logdebug(LOGGER, msg+' yet, as the module has not been started yet.')

        elif self.statemachine.is_permanently_unavailable():

            if self.statemachine.could_not_connect:
                logtrace(LOGGER, msg+', as the connection failed.')
                if self.__have_not_warned_about_connection_fail_yet:
                    logwarn(LOGGER, msg+'. The connection failed definitively.')
                    self.__have_not_warned_about_connection_fail_yet = False

            elif self.statemachine.closed_by_publisher:
                logtrace(LOGGER, msg+', as the connection was closed by the user.')
                if self.__have_not_warned_about_force_close_yet:
                    logwarn(LOGGER, msg+'. The sender was force closed.')
                    self.__have_not_warned_about_force_close_yet = False

    def __trigger_one_publish_action(self):
        logtrace(LOGGER, 'Next message will be sent in %d seconds.', self.__PUBLISH_INTERVAL_SECONDS)
        self.thread._connection.add_timeout(self.__PUBLISH_INTERVAL_SECONDS, self.feeder.publish_message)
        logtrace(LOGGER, '(Trigger sent.)')

    def on_message_not_accepted(self, channel, method, props, body):
        try:
            logging.info('The message was returned.')
            logging.info('Return was: %s', method)
            logging.info('Body: %s', body)
            body_json = json.loads(body)
            try:
                body_json['original_routing_key'] = body_json[messages.JSON_KEY_ROUTING_KEY]
            except KeyError:
                body_json['original_routing_key'] = 'None'
            body_json[messages.JSON_KEY_ROUTING_KEY] = defaults.RABBIT_EMERGENCY_ROUTING_KEY # cmip6.publisher.HASH.emergency
            self.__resend_an_unroutable_message(json.dumps(body_json))

        except pika.exceptions.ChannelClosed as e:
            logging.trace('Could not execute callback: %e', e.message)
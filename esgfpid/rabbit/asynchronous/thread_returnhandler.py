import logging
import pika
import json
import esgfpid.defaults as defaults
import esgfpid.assistant.messages
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class UnacceptedMessagesHandler(object):

    def __init__(self, thread):
        self.thread = thread

        self.__have_warned_about_double_unroutable_already = False

    '''
    Callback, called by RabbitMQ.
    '''
    def on_message_not_accepted(self, channel, returned_frame, props, body):
        # Messages that are returned are confirmed anyways.
        # If we sent 20 messages that are returned, all 20 are acked,
        # so we do not need to retrieve them from the unconfirmed
        # messages after resending.
        # In the end, we'll have published 40 messages and received 40 acks.

        # Logging...
        logtrace(LOGGER, 'Return frame: %s', returned_frame) # <Basic.Return(['exchange=rabbitsender_integration_tests', 'reply_code=312', 'reply_text=NO_ROUTE', 'routing_key=cmip6.publisher.HASH.cart.datasets'])>
        logtrace(LOGGER, 'Return props: %s', props)  # <BasicProperties(['content_type=application/json', 'delivery_mode=2'])>
        logtrace(LOGGER, 'Return body: %s', body)

        # Was it the first or second time it comes back?
        if returned_frame.reply_text == 'NO_ROUTE':
            loginfo(LOGGER, 'The message was returned because it could not be assigned to any queue. No binding for routing key "%s".', returned_frame.routing_key)
            if returned_frame.routing_key.startswith(defaults.RABBIT_EMERGENCY_ROUTING_KEY):
                self.__log_about_double_return(returned_frame, body)
            else:
                self.__resend_message(returned_frame, props, body)
        else:
            logerror(LOGGER, 'The message was returned. Routing key: %s. Unknown reason: %s', returned_frame.routing_key, returned_frame.reply_text)
            self.__resend_message(returned_frame, props, body)

    def __log_about_double_return(self, frame, body):
        if not self.__have_warned_about_double_unroutable_already:
            body_json = json.loads(body)
            logerror(LOGGER, 'The RabbitMQ node refused a message a second time (with the original routing key "%s" and the emergency routing key "%s"). Dropping the message.',
                body_json['original_routing_key'], frame.routing_key)
            self.__have_warned_about_double_unroutable_already = True
        logdebug(LOGGER, 'This is the second time the message comes back. Dropping it.')

    def __resend_message(self, returned_frame, props, body):
        try:
            body_json = json.loads(body)
            body_json = self.__add_emergency_routing_key(body_json)
            self.__resend_an_unroutable_message(json.dumps(body_json))
        except pika.exceptions.ChannelClosed as e:
            logdebug(LOGGER, 'Error during "on_message_not_accepted": %s: %s', e.__class__.__name__, e.message)
            logerror(LOGGER, 'Could not resend message: %s: %s', e.__class__.__name__, e.message)

    def __add_emergency_routing_key(self, body_json):
        emergency_routing_key = defaults.RABBIT_EMERGENCY_ROUTING_KEY
        key_for_routing_key = esgfpid.assistant.messages.JSON_KEY_ROUTING_KEY

        # If there was no routing key, set the original one to 'None'
        if key_for_routing_key not in body_json:
            logerror(LOGGER, 'Very unexpected: RabbitMQ returned a message that had no routing key: %s', body_json)
            body_json[key_for_routing_key] = 'None'

        # If it already HAS the emergency routing key, do not adapt the routing key
        # (This means the message already came back a second time...)
        if body_json[key_for_routing_key] == emergency_routing_key:
            pass

        # Otherwise, store the original one in another field...
        # and overwrite it by the emergency routing key:
        else:
            body_json['original_routing_key'] = body_json[key_for_routing_key]
            logdebug(LOGGER, 'Adding emergency routing key %s', emergency_routing_key)
            body_json[key_for_routing_key] = emergency_routing_key
        return body_json

    def __resend_an_unroutable_message(self, message):
        logdebug(LOGGER, 'Resending message...')
        self.thread.send_a_message(message)
import logging
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class UnacceptedMessagesHandler(object):

    def __init__(self, thread):
        self.thread = thread

    '''
    Callback, called by RabbitMQ.
    '''
    def on_message_not_accepted(self, channel, method, props, body):
        # TODO:
        # Do we know the message's delivery tag?
        # Then we can use that to retrieve it from the confirmer's
        # Unconfirmed messages. That is necessary, as by republishing
        # it, we're putting it back into the unconfirmed messages.
        try:
            loginfo(LOGGER,'The message was returned.')
            loginfo(LOGGER,'Return was: %s', method)
            loginfo(LOGGER,'Body: %s', body)
            body_json = json.loads(body)
            body_json = self.__add_emergency_routing_key(body_json)
            self.__resend_an_unroutable_message(json.dumps(body_json))

        except pika.exceptions.ChannelClosed as e:
            logdebug(LOGGER, 'Error during "on_message_not_accepted": %s', e.message)
            logerror(LOGGER, 'Could not resend message: %s', e.message)

    def __add_emergency_routing_key(self, body_json):

        # If it already HAS the emergency routing key, do not adapt the routing key
        # (This means the message already came back a second time...)
        if body_json[messages.JSON_KEY_ROUTING_KEY] == defaults.RABBIT_EMERGENCY_ROUTING_KEY:
            pass

        # Otherwise, store the original one in another field...
        # and overwrite it by the emergency routing key:
        else:
            try:
                body_json['original_routing_key'] = body_json[messages.JSON_KEY_ROUTING_KEY]

            # If there was no routing key, set the original one to 'None'
            except KeyError:
                body_json['original_routing_key'] = 'None'


            body_json[messages.JSON_KEY_ROUTING_KEY] = defaults.RABBIT_EMERGENCY_ROUTING_KEY
        return body_json

    def __resend_an_unroutable_message(self, message):
            self.thread.send_a_message(message)
import json
import random
import logging
import esgfpid.defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
Retrieves the routing key from the message, checks
the message's validity and returns
the string message and the routing key string.

If message is string, convert to JSON (dictionary).
If message is dictionary, check if it's valid JSON.
Retrieve the routing key, which should be included
with the key "ROUTING_KEY". Otherwise, a default
routing key is added.

:param msg: Message to be sent to RabbitMQ as JSON
    string or dictionary.
:return: Routing key string and string message as tuple.
'''
def get_routing_key_and_string_message_from_message_if_possible(msg):

    # Try to convert message to json:
    json_ok = False
    msg_json = None
    msg_string = None
    
    if msg is None:
        raise ValueError('The message that was passed is None.')
    
    # Get JSON from message, if possible!
    if isinstance(msg, basestring):

        try:
            # Valid string message --> JSON
            msg_string = msg
            msg_json = json.loads(msg)
            json_ok = True
            logdebug(LOGGER, 'Message was transformed to json.')
        except ValueError as e:

            # Invalid string message
            loginfo(LOGGER, 'Message seems to be invalid json: %s', msg)
            msg_string = str(msg)
            json_ok = False
    else:
        try:
            # Message is json already.
            msg_string = json.dumps(msg)
            msg_json = msg
            json_ok = True
            logtrace(LOGGER, 'Message was already json.')

        except TypeError as e:
            if 'not JSON serializable' in e.message:

                # Message was whatever.
                msg_string = str(msg)
                json_ok = False
                msg = ('Message was neither JSON nor string and not understandable: %s' % msg_string)
                loginfo(LOGGER, msg)
                raise ValueError(msg)


    # If we succeeded, try to get routing key:
    routing_key = None
    if json_ok:
        try:
            routing_key = msg_json['ROUTING_KEY']
            logtrace(LOGGER, 'Routing key extracted from message.')
        except (KeyError, TypeError) as e:
            logdebug(LOGGER, 'No routing key in message.')
            routing_key = esgfpid.defaults.RABBIT_DEFAULT_ROUTING_KEY
            pass # There is no routing key in the message
    else:
        routing_key = esgfpid.defaults.RABBIT_DEFAULT_ROUTING_KEY

    return routing_key, msg_string


def add_emergency_routing_key(body_json):
    emergency_routing_key = esgfpid.defaults.RABBIT_EMERGENCY_ROUTING_KEY

    # If it already HAS the emergency routing key, do not adapt the routing key
    # (This means the message already came back a second time...)
    if body_json[esgfpid.assistant.messages.JSON_KEY_ROUTING_KEY] == emergency_routing_key:
        pass

    # Otherwise, store the original one in another field...
    # and overwrite it by the emergency routing key:
    else:
        try:
            body_json['original_routing_key'] = body_json[esgfpid.assistant.messages.JSON_KEY_ROUTING_KEY]

        # If there was no routing key, set the original one to 'None'
        except KeyError:
            logerror('Very unexpected: RabbitMQ returned a message that had no routing key: %s', body_json)
            body_json['original_routing_key'] = 'None'

        logdebug(LOGGER, 'Adding emergency routing key %s', emergency_routing_key)
        body_json[esgfpid.assistant.messages.JSON_KEY_ROUTING_KEY] = emergency_routing_key
    return body_json, emergency_routing_key
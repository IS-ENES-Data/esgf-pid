import json
import esgfpid.defaults
import random
import logging
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

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


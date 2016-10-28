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


def ensure_urls_are_a_list(args, LOGGER):
    if isinstance(args['urls_fallback'], basestring):
        args['urls_fallback'] = [args['urls_fallback']]
    elif args['urls_fallback'] is None:
        args['urls_fallback'] = []
    elif not isinstance(args['urls_fallback'], (list, tuple)):
        msg = ('Rabbit URLs are neither list nor string, but %s: %s' %
               (type(args['urls_fallback']), args['urls_fallback']))
        raise ValueError(msg)

def set_preferred_url(args, LOGGER):
    if args['url_preferred'] is None:
        _select_fallback_url_as_preferred(args, LOGGER)

def _select_fallback_url_as_preferred(args, LOGGER):
    if len(args['urls_fallback']) == 1: # for this, it HAS to be a list! Otherwise, string length is counted.
        _select_only_fallback_url_as_preferred(args)
        logdebug(LOGGER, 'The only provided URL is: %s', args['url_preferred'])
    else:
        _select_random_fallback_url_as_preferred(args)            
        loginfo(LOGGER, 'No preferred messaging service URL provided. Randomly selected %s.', args['url_preferred'])

def _select_only_fallback_url_as_preferred(args):
    args['url_preferred'] = args['urls_fallback'].pop()

def _select_random_fallback_url_as_preferred(args):
    num_urls = len(args['urls_fallback'])
    random_num = random.randint(0,num_urls-1)
    random_preferred_url = args['urls_fallback'][random_num]
    args['url_preferred'] = random_preferred_url
    args['urls_fallback'].remove(random_preferred_url)

def ensure_no_duplicate_urls(args, unused_logger):
    args['urls_fallback'] = list(set(args['urls_fallback']))
    if args['url_preferred'] in args['urls_fallback']:
        args['urls_fallback'].remove(args['url_preferred'])

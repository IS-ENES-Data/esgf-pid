'''
The ESGF PID system relies on a RabbitMQ instance consisting
of various exchanges and queues. Messages are routed to
these queues using routing keys.

This file contains the various routing keys that are used.

The esgfpid library uses routing keys for topic exchanges,
consisting of four "words" (separated by dots). The first
word is the prefix (but without dots), the second one is
the word "HASH" (placeholder for possibe future load balancing
using hash values), the third is an instruction for the Rabbit,
and the fourth one is a short description of the type of message
being sent.

This way, messages can be routed to different consumers based
on the prefix, they can be prioritized based on their content,
and filtered using some instructions.

'''


'''
This is the Rabbit instruction. It tells the RabbitMQ system
that the message received is a freshly published one, in
opposition to messages that have been parked and are retried
several times.
Please note that the instruction can be extended by the
functions below.
'''
RABBIT_INSTRUCTION = 'fresh'


'''
These are the templates for the routing keys, based on the
message's content.
'''
ROUTING_KEYS_TEMPLATES = dict(
    publi_file = 'PREFIX.HASH.'+RABBIT_INSTRUCTION+'.publi-file-orig',
    publi_file_rep = 'PREFIX.HASH.'+RABBIT_INSTRUCTION+'.publi-file-repli',
    publi_ds = 'PREFIX.HASH.'+RABBIT_INSTRUCTION+'.publi-ds-orig',
    publi_ds_rep = 'PREFIX.HASH.'+RABBIT_INSTRUCTION+'.publi-ds-repli',
    unpubli_all = 'PREFIX.HASH.'+RABBIT_INSTRUCTION+'.unpubli-allvers',
    unpubli_one = 'PREFIX.HASH.'+RABBIT_INSTRUCTION+'.unpubli-onevers',
    err_add = 'PREFIX.HASH.'+RABBIT_INSTRUCTION+'.errata-add',
    err_rem = 'PREFIX.HASH.'+RABBIT_INSTRUCTION+'.errata-rem',
    data_cart = 'PREFIX.HASH.'+RABBIT_INSTRUCTION+'.datacart',
    pre_flight = 'PREFIX.HASH.'+RABBIT_INSTRUCTION+'.preflightcheck'
)

'''
This is where the routing keys will be stored, when the
real prefix has been added.
'''
ROUTING_KEYS = dict()

'''
Helper: Remove all dots from the prefix so that it can
be used for routing keys. Dots in routing keys are word
separators.
'''
def _sanitize_prefix(prefix):
    prefix = prefix
    if 'hdl:' in prefix:
        prefix = prefix.replace('hdl:','')
    return prefix.replace('.', '')

'''
Add the (sanitized) prefix to all routing keys.
'''
def add_prefix_to_routing_keys(prefix):
    sanitized_prefix = _sanitize_prefix(prefix)
    for k,v in ROUTING_KEYS_TEMPLATES.iteritems():
        ROUTING_KEYS[k] = v.replace('PREFIX', sanitized_prefix)




'''
These three keywords may be added to the Rabbit instruction
part of the routing key, in case a message is not sent
via a trusted RabbitMQ node.
Currently, this should not happen, as we raise an exception
if an untrusted node is used.
'''
ROUTING_KEY_INTERFIX_UNSURE_IF_TRUSTED = 'untrusted-unsure'
ROUTING_KEY_INTERFIX_UNTRUSTED_AS_FALLBACK = 'untrusted-fallback'
ROUTING_KEY_INTERFIX_UNTRUSTED = 'untrusted-only'


def adapt_routing_key_for_untrusted(routing_key):
    routing_key = routing_key.replace('fresh', 'fresh-'+ROUTING_KEY_INTERFIX_UNTRUSTED)
    return routing_key

def adapt_routing_key_for_untrusted_fallback(routing_key):
    routing_key = routing_key.replace('fresh', 'fresh-'+ROUTING_KEY_INTERFIX_UNTRUSTED_AS_FALLBACK)
    return routing_key

def adapt_routing_key_for_untrusted_unsure(routing_key):
    routing_key = routing_key.replace('fresh', 'fresh-'+ROUTING_KEY_INTERFIX_UNSURE_IF_TRUSTED)
    return routing_key

'''
The default routing key, in case no key is included
in the message. This can only happen if there is some
severe error in the library. Or in testing situations.
'''
RABBIT_DEFAULT_ROUTING_KEY='fallback.fallback.'+RABBIT_INSTRUCTION+'.fallback'

'''
The default routing key, in case a message comes back
from the RabbitMQ as "unroutable". This can only happen
if there is a severe problem with the RabbitMQ federation
(i.e. the exchanges and bindings are not correct, the
alternate-exchange did not catch the message).
'''
RABBIT_EMERGENCY_ROUTING_KEY='UNROUTABLE.UNROUTABLE.'+RABBIT_INSTRUCTION+'.UNROUTABLE'


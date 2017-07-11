
ROUTING_KEY_BASIS = 'cmip6.publisher.HASH.'

ROUTING_KEYS = dict(
    publi_file = 'PREFIX.HASH.fresh.publi-file-orig',
    publi_file_rep = 'PREFIX.HASH.fresh.publi-file-repli',
    publi_ds = 'PREFIX.HASH.fresh.publi-ds-orig',
    publi_ds_rep = 'PREFIX.HASH.fresh.publi-ds-repli',
    unpubli_all = 'PREFIX.HASH.fresh.unpubli-allvers',
    unpubli_one = 'PREFIX.HASH.fresh.unpubli-onevers',
    err_add = 'PREFIX.HASH.fresh.errata-add',
    err_rem = 'PREFIX.HASH.fresh.errata-rem',
    data_cart = 'PREFIX.HASH.fresh.datacart'
)

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

RABBIT_DEFAULT_ROUTING_KEY='fallback.fallback.fallback.fallback' # Default, if none is included in message
RABBIT_EMERGENCY_ROUTING_KEY='UNROUTABLE.UNROUTABLE.UNROUTABLE.UNROUTABLE' # If the message was returned as unroutable by the sender


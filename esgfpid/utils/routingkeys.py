
ROUTING_KEY_BASIS = 'cmip6.publisher.HASH.'

ROUTING_KEYS = dict(
    publi_file = ROUTING_KEY_BASIS+'publication.file.orig',
    publi_file_rep = ROUTING_KEY_BASIS+'publication.file.replica',
    publi_ds = ROUTING_KEY_BASIS+'publication.dataset.orig',
    publi_ds_rep = ROUTING_KEY_BASIS+'publication.dataset.replica',
    unpubli_all = ROUTING_KEY_BASIS+'unpublication.all',
    unpubli_one = ROUTING_KEY_BASIS+'unpublication.one',
    err_add = ROUTING_KEY_BASIS+'errata.add',
    err_rem = ROUTING_KEY_BASIS+'errata.remove',
    shop_cart = ROUTING_KEY_BASIS+'cart.datasets'
)


ROUTING_KEY_SUFFIX_TRUSTED = 'trusted'
ROUTING_KEY_SUFFIX_UNSURE_IF_TRUSTED = 'untrusted-unsure'
ROUTING_KEY_SUFFIX_UNTRUSTED_AS_FALLBACK = 'untrusted-fallback'
ROUTING_KEY_SUFFIX_UNTRUSTED = 'untrusted-only'

RABBIT_DEFAULT_ROUTING_KEY=ROUTING_KEY_BASIS+'fallback' # Default, if none is included in message
RABBIT_EMERGENCY_ROUTING_KEY='UNROUTABLE' # If the message was returned as unroutable by the sender
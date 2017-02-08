import esgfpid.utils
from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS

'''
The messages module creates the JSON messages to be sent to the rabbit.
It does not check the type of the values passed. This has to be checked in the assistants,
otherwise it is redundant.
'''

JSON_KEY_ROUTING_KEY = 'ROUTING_KEY'

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

def publish_file(**args):

    # Check args:
    mandatory_args = ['file_handle', 'is_replica', 'file_size', 'file_name', 'checksum',
                      'data_url', 'parent_dataset', 'timestamp', 'checksum_type',
                    'file_version', 'data_node']
    esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

    # Message:
    message = dict(
        handle = args['file_handle'],
        aggregation_level = 'file',
        operation = 'publish',
        is_replica=args['is_replica'],
        file_name=args['file_name'],
        file_size=args['file_size'],
        checksum=args['checksum'],
        data_url=args['data_url'],
        data_node=args['data_node'],
        parent_dataset=args['parent_dataset'],
        message_timestamp = args['timestamp'],
        checksum_type = args['checksum_type'],
        file_version = args['file_version'] # can be int or string or ...
    )

    # Routing key:
    routing_key = ROUTING_KEYS['publi_file']
    if args['is_replica'] == True: # Publish Assistant parses this to boolean!
        routing_key = ROUTING_KEYS['publi_file_rep']
    message[JSON_KEY_ROUTING_KEY] = routing_key

    return message

def publish_dataset(**args):

    # Check args:
    mandatory_args = ['dataset_handle', 'drs_id', 'is_replica', 'version_number',
                      'list_of_files', 'data_node', 'timestamp']
    esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

    # Message:
    message = dict(
        handle = args['dataset_handle'],
        aggregation_level = 'dataset',
        operation = 'publish',
        drs_id = args['drs_id'],
        is_replica=args['is_replica'],
        version_number=args['version_number'], # Publish Assistant parses this to int!
        files=args['list_of_files'],
        data_node=args['data_node'],
        message_timestamp = args['timestamp']
    )

    # Optional:
    if 'consumer_solr_url' in args and args['consumer_solr_url'] is not None:
        message['consumer_solr_url'] = args['consumer_solr_url']

    # Routing key:
    routing_key = ROUTING_KEYS['publi_ds']
    if args['is_replica'] == True: # Publish Assistant parses this to boolean!
        routing_key = ROUTING_KEYS['publi_ds_rep']
    message[JSON_KEY_ROUTING_KEY] = routing_key

    return message

def unpublish_allversions_consumer_must_find_versions(**args):

    mandatory_args = ['drs_id', 'data_node', 'timestamp']
    esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

    message = dict(
        operation = 'unpublish_all_versions',
        aggregation_level = 'dataset',
        message_timestamp = args['timestamp'],
        drs_id = args['drs_id'],
        data_node=args['data_node']
    )

    # Optional:
    if 'consumer_solr_url' in args and args['consumer_solr_url'] is not None:
        message['consumer_solr_url'] = args['consumer_solr_url']

    message[JSON_KEY_ROUTING_KEY] = ROUTING_KEYS['unpubli_all']
    return message


def unpublish_one_version(**args):

    mandatory_args = ['data_node', 'timestamp', 'dataset_handle', 'drs_id']
    esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

    message = dict(
        operation = 'unpublish_one_version',
        aggregation_level = 'dataset',
        message_timestamp = args['timestamp'],
        handle = args['dataset_handle'],
        data_node=args['data_node'],
        drs_id = args['drs_id']
    )
    if 'version_number' in args:
        message['version_number'] = args['version_number']

    message[JSON_KEY_ROUTING_KEY] = ROUTING_KEYS['unpubli_one']

    return message

def add_errata_ids_message(**args):

    mandatory_args = ['dataset_handle', 'timestamp', 'errata_ids', 'drs_id', 'version_number']
    esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
  
    message = dict(
        handle = args['dataset_handle'],
        message_timestamp = args['timestamp'],
        errata_ids = args['errata_ids'],
        operation = 'add_errata_ids',
        drs_id = args['drs_id'],
        version_number = args['version_number']
    )

    message[JSON_KEY_ROUTING_KEY] = ROUTING_KEYS['err_add']

    return message

def remove_errata_ids_message(**args):

    mandatory_args = ['dataset_handle', 'timestamp', 'errata_ids', 'drs_id', 'version_number']
    esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
    
    message = dict(
        handle = args['dataset_handle'],
        message_timestamp = args['timestamp'],
        errata_ids = args['errata_ids'],
        operation = 'remove_errata_ids',
        drs_id = args['drs_id'],
        version_number = args['version_number']
    )
    
    message[JSON_KEY_ROUTING_KEY] = ROUTING_KEYS['err_rem']

    return message

def make_data_cart_message(**args):
    mandatory_args = ['cart_handle', 'timestamp', 'data_cart_content']
    esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
    message = dict(
        handle = args['cart_handle'],
        message_timestamp = args['timestamp'],
        data_cart_content = args['data_cart_content'],
        operation = 'shopping_cart'
    )
    message[JSON_KEY_ROUTING_KEY] = ROUTING_KEYS['shop_cart']
    return message
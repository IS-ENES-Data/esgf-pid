import esgfpid.utils

'''
The messages module creates the JSON messages to be sent to the rabbit.
It does not check the type of the values passed. This has to be checked in the assistants,
otherwise it is redundant.
'''

JSON_KEY_ROUTING_KEY = 'ROUTING_KEY'

def publish_file(**args):

    # Check args:
    mandatory_args = ['file_handle', 'is_replica', 'file_size', 'file_name', 'checksum',
                      'data_url', 'parent_dataset', 'timestamp', 'checksum_type', 'file_version']
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
        parent_dataset=args['parent_dataset'],
        message_timestamp = args['timestamp'],
        checksum_type = args['checksum_type'],
        file_version = args['file_version'] # can be int or string or ...
    )

    # Routing key:
    routing_key = 'publication_file'
    if args['is_replica'] == True: # Publish Assistant parses this to boolean!
        routing_key = 'publication_file_replica'
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
    routing_key = 'publication_dataset'
    if args['is_replica'] == True: # Publish Assistant parses this to boolean!
        routing_key = 'publication_dataset_replica'
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

    routing_key = 'unpublish_all_versions'
    message[JSON_KEY_ROUTING_KEY] = routing_key
    return message


def unpublish_one_version(**args):

    mandatory_args = ['data_node', 'timestamp', 'dataset_handle']
    esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

    message = dict(
        operation = 'unpublish_one_version',
        aggregation_level = 'dataset',
        message_timestamp = args['timestamp'],
        handle = args['dataset_handle'],
        data_node=args['data_node']
    )

    routing_key = 'unpublish_one_version'
    message[JSON_KEY_ROUTING_KEY] = routing_key

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

    routing_key = 'errata_ids'
    message[JSON_KEY_ROUTING_KEY] = routing_key

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
    
    routing_key = 'errata_ids'
    message[JSON_KEY_ROUTING_KEY] = routing_key

    return message

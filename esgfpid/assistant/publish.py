import logging
import esgfpid.exceptions
import esgfpid.assistant.consistency
import esgfpid.assistant.messages
import esgfpid.utils as utils
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Helper:

def create_dataset_handle(**args):
    return utils.make_handle_from_drsid_and_versionnumber(
        drs_id=args['drs_id'],
        version_number=args['version_number'],
        prefix=args['prefix']
    )

class DatasetPublicationAssistant(object):

    def __init__(self, **args):
        logdebug(LOGGER, 'Constructor for Publication assistant for dataset "%s", version "%s" at host "%s".',
            args['drs_id'],
            args['version_number'],
            args['data_node']
        )

        # Check args
        mandatory_args = ['drs_id', 'version_number', 'data_node', 'prefix',
                          'thredds_service_path', 'is_replica', 'coupler',
                          'consumer_solr_url']
        optional_args = []
        utils.check_presence_of_mandatory_args(args, mandatory_args)
        utils.add_missing_optional_args_with_value_none(args, optional_args)
        self.__enforce_integer_version_number(args)
        self.__enforce_boolean_replica_flag(args)

        # Init methods...
        self.__store_args_in_attributes(args)
        self.__define_other_attributes()
        self.__create_and_store_dataset_handle()
        self.__init_state_machine()

        logdebug(LOGGER, 'Done: Constructor for Publication assistant for dataset "%s", version "%i" at host "%s".',
            args['drs_id'],
            args['version_number'],
            args['data_node']
        )

    def __enforce_integer_version_number(self, args):
        try:
            args['version_number'] = int(args['version_number'])
        except ValueError:
            raise esgfpid.exceptions.ArgumentError('Dataset version number is not an integer')

    def __enforce_boolean_replica_flag(self, args):
        try:
            args['is_replica'] = utils.get_boolean(args['is_replica'])
        except ValueError:
            msg = ('Replica flag "%s" could not be parsed to boolean. '
                   'Please pass a boolean or "True" or "true" or "False" or "false"'
                   % args['is_replica'])
            raise esgfpid.exceptions.ArgumentError(msg)

    def __enforce_integer_file_size(self, args):
        try:
            args['file_size'] = int(args['file_size'])
        except ValueError:
            raise esgfpid.exceptions.ArgumentError('File size is not an integer')

    def __enforce_string_file_version(self, args):
        args['file_version'] = str(args['file_version'])

    def __define_other_attributes(self):
        self.__dataset_handle = None
        self.__list_of_file_handles = []
        self.__list_of_file_messages = []
        self.__message_timestamp = utils.get_now_utc_as_formatted_string()

    def __store_args_in_attributes(self, args):
        self.__drs_id = args['drs_id']
        self.__version_number = args['version_number']
        self.__data_node = args['data_node'].rstrip('/')
        self.__prefix = args['prefix']
        self.__thredds_service_path = args['thredds_service_path'].strip('/')
        self.__is_replica = args['is_replica']
        self.__coupler = args['coupler']
        self.__consumer_solr_url = args['consumer_solr_url']

    def __init_state_machine(self):
        self.__machine_states = {'dataset_added':0, 'files_added':1, 'publication_finished':2}
        self.__machine_state = self.__machine_states['dataset_added']

    def __create_and_store_dataset_handle(self):
        self.__dataset_handle = create_dataset_handle(
            drs_id = self.__drs_id,
            version_number = self.__version_number,
            prefix = self.__prefix
        )

    def get_dataset_handle(self):
        '''
        This returns the handle string of the dataset to be
        published, so that the publisher can use it for its
        own purposes, e.g. publishing it on a website.

        The handle string consists of the prefix specified
        at library init, and a suffix. The suffix is created
        by making a hash over dataset id and version number.

        :return: The handle string of this dataset,
            e.g.: "hdl:21.14100/foobar".
        '''
        return self.__dataset_handle

    # work horses:

    def add_file(self, **args):
        '''
        Adds a file's information to the set of files to be
        published in this dataset.

        :param file_name: Mandatory. The file name (string).
            This information will simply be included in the
            PID record, but not used for anything.

        :param file_handle: Mandatory. The handle (PID) of
            this file (string). It is included in the file's netcdf
            header. It must bear the prefix that this library 
            (or rather, the consuming servlet that will consume
            this library's requests), has write access to.

        :param file_size: Mandatory. The file size (as string or
            integer. Will be transformed to integer). This
            information will be included in the handle record
            and used for consistency checks during republications
            of files with the same handle.

        :param checksum: Mandatory. The file's checksum. This
            information will be included in the handle record
            and used for consistency checks during republications
            of files with the same handle.

        :param checksum_type: Mandatory. The checksum type/method
            (string), e.g. "MD5" or "SHA256". This information will
            be included in the handle record and used for consistency
            checks during republications of files with the same handle.

        :param publish_path: Mandatory. The THREDDS publish path as
            a string. This is part of the URL for accessing the file,
            which will be part of the handle record. It will not be
            accessed, neither by the library nor by the consumer.
            The URL consists of the dataset's "data_node", the dataset's 
            "thredds_service_path", and this "publish_path". Redundant
            slashes are removed. If the URL does not start with "http",
            "http://" is added.

        :param file_version: Mandatory. Any string. File versions
            are not managed in the PID. This information will simply be
            included in the PID record, but not used for any reasoning.
        '''

        # Check if allowed:
        self.__check_if_adding_files_allowed_right_now()

        # Check if args ok:
        mandatory_args = ['file_name', 'file_handle', 'file_size',
                          'checksum', 'publish_path', 'checksum_type',
                          'file_version']
        utils.check_presence_of_mandatory_args(args, mandatory_args)
        self.__enforce_integer_file_size(args)
        self.__enforce_string_file_version(args)

        # Add file:
        self.__check_and_correct_handle_syntax(args)
        self.__add_file(**args)

    def __check_and_correct_handle_syntax(self, args):
        self.__make_sure_hdl_is_added(args)
        self.__check_if_prefix_is_there(args['file_handle'])

    def __add_file(self, **args):
        logdebug(LOGGER, 'Adding file "%s" with handle "%s".', args['file_name'], args['file_handle'])
        self.__add_file_to_datasets_children(args['file_handle'])
        self.__adapt_file_args(args)
        self.__create_and_store_file_publication_message(args)        
        self.__set_machine_state_to_files_added()
        logtrace(LOGGER, 'Adding file done.')

    def __add_file_to_datasets_children(self, file_handle):
        self.__list_of_file_handles.append(file_handle)

    def __make_sure_hdl_is_added(self, args):
        if not args['file_handle'].startswith('hdl:'):
            args['file_handle'] = 'hdl:'+args['file_handle']

    def __check_if_prefix_is_there(self, file_handle):
        if not file_handle.startswith('hdl:'+self.__prefix+'/'):

            expected = self.__prefix + '/'+ file_handle.lstrip('hdl:')
            msg = ('\nThis file\'s tracking_id "%s" does not have the expected handle prefix "%s".'
                   '\nExpected "%s" or "%s"' % (file_handle, self.__prefix, expected, 'hdl:'+expected))

            if '/' in file_handle:
                maybe_prefix = file_handle.split('/')[0].lstrip('hdl:')
                msg += ('.\nIf "%s" was meant to be the prefix, it does not correspond to '
                        'the prefix specified when initializing this library' % maybe_prefix)

            raise esgfpid.exceptions.ESGFException(msg)


    def __adapt_file_args(self, args):
        url = self.__create_file_url(args['publish_path'])
        args['data_url'] = url
        del args['publish_path']

    def __create_file_url(self, publish_path):
        url = self.__data_node +'/'+ self.__thredds_service_path +'/'+ publish_path.strip('/')
        if not url.startswith('http'):
            url = 'http://'+url
        return url

    def __create_and_store_file_publication_message(self, args):
        message = self.__create_file_publication_message(args)
        self.__list_of_file_messages.append(message)

    def __set_machine_state_to_files_added(self):
        self.__machine_state = self.__machine_states['files_added']

    def __check_if_adding_files_allowed_right_now(self):
        dataset_added = self.__machine_state == self.__machine_states['dataset_added']
        files_added = self.__machine_state == self.__machine_states['files_added']
        if dataset_added or files_added:
            pass
        else:
            msg = 'Too late to add files!'
            logwarn(LOGGER, msg)
            raise esgfpid.exceptions.OperationUnsupportedException(msg)

    def dataset_publication_finished(self, ignore_exception=False):
        '''
        This is the "commit". It triggers the creation/update of handles.
        
        * Check if the set of files corresponds to the previously published set (if applicable, and if solr url given, and if solr replied)
        * The dataset publication message is created and sent to the queue.
        * All file publication messages are sent to the queue.

        '''
        self.__check_if_dataset_publication_allowed_right_now()
        self.__check_data_consistency(ignore_exception)
        self.__coupler.start_rabbit_business() # Synchronous: Opens connection. Asynchronous: Ignored.
        self.__create_and_send_dataset_publication_message_to_queue()
        self.__send_existing_file_messages_to_queue()
        self.__coupler.done_with_rabbit_business() # Synchronous: Closes connection. Asynchronous: Ignored.
        self.__set_machine_state_to_finished()
        loginfo(LOGGER, 'Requesting to publish PID for dataset "%s" (version %s) and its files at "%s" (handle %s).', self.__drs_id, self.__version_number, self.__data_node, self.__dataset_handle)

    def __check_if_dataset_publication_allowed_right_now(self):
        if not self.__machine_state == self.__machine_states['files_added']:
            msg = None
        
            if self.__machine_state == self.__machine_states['dataset_added']:
                msg = 'No file added yet.'
            else:
                msg = 'Publication was already done.'

            logwarn(LOGGER, msg)
            raise esgfpid.exceptions.OperationUnsupportedException(msg)


    def __check_data_consistency(self, ignore_exception):
        checker = esgfpid.assistant.consistency.Checker(
            coupler=self.__coupler,
            drs_id=self.__drs_id,
            version_number=self.__version_number,
            data_node=self.__data_node
        )
        check_possible = checker.can_run_check()
        if check_possible:
            check_passed = checker.data_consistency_check(self.__list_of_file_handles)
            if check_passed:
                loginfo(LOGGER, 'Data consistency check passed for dataset %s.', self.__dataset_handle)
            else:
                msg = 'Dataset consistency check failed'
                logwarn(LOGGER, msg)
                if not ignore_exception:
                    raise esgfpid.exceptions.InconsistentFilesetException(msg)
        else:
            logdebug(LOGGER, 'No consistency check was carried out.')

    def __create_and_send_dataset_publication_message_to_queue(self):
        self.__remove_duplicates_from_list_of_file_handles()
        message = self.__create_dataset_publication_message()
        self.__send_message_to_queue(message)
        logdebug(LOGGER, 'Dataset publication message handed to rabbit thread.')
        logtrace(LOGGER, 'Dataset publication message: %s (%s, version %s).', self.__dataset_handle, self.__drs_id, self.__version_number)

    def __remove_duplicates_from_list_of_file_handles(self):
        self.__list_of_file_handles = list(set(self.__list_of_file_handles))

    def __send_existing_file_messages_to_queue(self):
        for i in xrange(0, len(self.__list_of_file_messages)):
            self.__try_to_send_one_file_message(i)
        msg = 'All file publication jobs handed to rabbit thread.'
        logdebug(LOGGER, msg)
        
    def __try_to_send_one_file_message(self, list_index):
        msg = self.__list_of_file_messages[list_index]
        success = self.__send_message_to_queue(msg)
        logdebug(LOGGER, 'File publication message handed to rabbit thread: %s (%s)', msg['handle'], msg['file_name'])
        return success

    def __set_machine_state_to_finished(self):
        self.__machine_state = self.__machine_states['publication_finished']

    def __create_file_publication_message(self, args):
        
        message = esgfpid.assistant.messages.publish_file(
            file_handle=args['file_handle'],
            file_size=args['file_size'],
            file_name=args['file_name'],
            checksum=args['checksum'],
            data_url=args['data_url'],
            data_node=self.__data_node,
            parent_dataset=self.__dataset_handle,
            checksum_type=args['checksum_type'],
            file_version=args['file_version'],
            is_replica=self.__is_replica,
            timestamp=self.__message_timestamp,
        )
        return message

    def __create_dataset_publication_message(self):

        message = esgfpid.assistant.messages.publish_dataset(
            dataset_handle=self.__dataset_handle,
            is_replica=self.__is_replica,
            drs_id=self.__drs_id,
            version_number=self.__version_number,
            list_of_files=self.__list_of_file_handles,
            data_node=self.__data_node,
            timestamp=self.__message_timestamp,
            consumer_solr_url=self.__consumer_solr_url
        )
        return message

    def __send_message_to_queue(self, message):
        success = self.__coupler.send_message_to_queue(message)
        return success
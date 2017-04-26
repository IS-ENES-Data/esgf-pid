'''
This module represents the API of the ESGF PID module.

Author: Merret Buurman (DKRZ), 2015-2016

'''

import logging
import esgfpid.assistant.publish
import esgfpid.assistant.unpublish
import esgfpid.assistant.errata
import esgfpid.assistant.datacart
import esgfpid.defaults
import esgfpid.exceptions
import esgfpid.coupling
import esgfpid.utils
from esgfpid.utils import loginfo, logdebug, logwarn

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class Connector(object):
    '''
    This class provides the main functionality for the ESGF PID
    module. 

    Author: Merret Buurman (DKRZ), 2015-2016
    '''

    def __init__(self, **args):
        '''
        Create a connector object with the necessary config
        to connect to a RabbitMQ messaging server and perform
        PID creations/updates.

        The arguments have to be passed as named parameters.
        Please contact your ESGF index node or CDNOT for this
        information.

        Some of the arguments are needed for making connections
        from this library to RabbitMQ or to solr. Other arguments
        are only passed on to the consuming servlet inside the
        messages.

        :param handle_prefix: Mandatory. The handle prefix (as a
            string) for the handles to be created/updates. This
            has to match the handle prefix that the message queue
            consuming servlet has write access to. In CMIP6, this
            is "21.14100".
        
        :param messaging_service_exchange_name: Mandatory. The
            name of the messaging exchange that will forward the
            messages to a specific queue.

        :param messaging_service_credentials: Mandatory. List of
            dictionaries with credentials for the RabbitMQ nodes.
            Each needs to have the entries: "user", "password", "url".
            They may have an integer "priority" too. If two nodes have
            the same priority, the library chooses randomly between
            them. They also may have a "vhost" (RabbitMQ virtual host),
            a "port" and a boolean "ssl_enabled". Please refer to pika's
            documentation
            (http://pika.readthedocs.io/en/latest/modules/parameters.html).
            Dictionaries for 'open nodes' do not need a password
            to be provided. Open nodes are only used if no more
            other nodes are available. Note: Open nodes are no longer
            supported.

        :param message_service_synchronous: Optional. Boolean to
            define if the connection to RabbitMQ and the message
            sending should work in synchronous mode. Defaults to
            the value defined in defaults.py.

        :param data_node: Mandatory/Optional.

            (Mandatory for publications and unpublications,
            ignored for any other usage of the library. No default.)

            The data node (host name) at which (un)publication takes
            place. This will be included in the handle records. Trailing
            slashes are removed.

            Used during publication and unpublication (modules
            assistant.publish and assistant.unpublish):

            * Publication: Used to construct the file data URL (together
              with thredds service path and file publish path). Sent along
              in rabbit message. Used for consistency check, if solr use 
              is enabled.
            * Unpublication: Sent along in rabbit message.
        
        :param thredds_service_path: Mandatory for publications,
            ignored for any other usage of the library. No default.
            The thredds service path where the files of a publication
            reside. Will be combined with files' publish path and data
            node to form the files' data access URLs.

        :param solr_url: Optional. The URL of the solr to be uses by this
            library for the dataset consistency check. No default. If not provided,
            the check is not done.
            Note: This is currently switched off for performance reasons.

        :param solr_https_verify: Optional flag to indicate whether
            requests to solr should verify the SSL certificate.
            Please see documentation of requests library: http://docs.python-requests.org/en/master/user/advanced/

        :param disable_insecure_request_warning: Optional flag (only for
            use during testing). If True, warnings are not printed during
            insecure SSL requests.
            Important: This is not passed through to the solr module, so
            that switching off the warnings is not possible. It can only
            be passed directly to the solr module during tests.

        :param solr_switched_off: Optional flag to tell if the solr module
            should be switched off. In that case, no connections to solr
            are made. 

        :param consumer_solr_url: Optional. URL of a solr instance that
            is to be used by the consumer (e.g. for finding versions), *not*
            by this library.

        :param test_publication: Optional flag. If True, the
            handles that are created are test handles
            that will be overwritten by real publications. Also,
            test publications cannot update real handles.

        :returns: An instance of the connector, configured for one 
            data node, and for connection with a specific RabbitMQ node.

        '''
        LOGGER.debug(40*'-')
        LOGGER.debug('Creating PID connector object ..')
        self.__check_presence_of_args(args)
        self.__check_rabbit_credentials_completeness(args)
        self.__define_defaults_for_optional_args(args)
        self.__store_some_args(args)
        self.__throw_error_if_prefix_not_in_list()
        self.__coupler = esgfpid.coupling.Coupler(**args)
        loginfo(LOGGER, 'Created PID connector.')

    def __check_presence_of_args(self, args):
        mandatory_args = [
            'messaging_service_credentials',
            'messaging_service_exchange_name',
            'handle_prefix'
        ]
        optional_args = [
            'data_node',
            'thredds_service_path',
            'test_publication',
            'solr_url',
            'solr_https_verify',
            'disable_insecure_request_warning',
            'solr_switched_off',
            'consumer_solr_url',
            'message_service_synchronous'
        ]
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

    def __define_defaults_for_optional_args(self, args):
        
        if 'data_node' not in args or args['data_node'] is None:
            ''' May be None, only needed for some operations.
                If it is needed, its presence is checked later. '''
            args['data_node'] = None

        if 'thredds_service_path' not in args or args['thredds_service_path'] is None:
            ''' May be None, only needed for some operations.
                If it is needed, its presence is checked later. '''
            args['thredds_service_path'] = None

        if 'test_publication' not in args or args['test_publication'] is None:
            args['test_publication'] = False

        if 'solr_url' not in args or args['solr_url'] is None:
            args['solr_url'] = None
            args['solr_switched_off'] = True

        if 'solr_switched_off' not in args or args['solr_switched_off'] is None:
            args['solr_switched_off'] = False

        if 'solr_https_verify' not in args or args['solr_https_verify'] is None:
            args['solr_https_verify'] = esgfpid.defaults.SOLR_HTTPS_VERIFY_DEFAULT

        if 'disable_insecure_request_warning' not in args or args['disable_insecure_request_warning'] is None:
            args['disable_insecure_request_warning'] = False

        if 'message_service_synchronous' not in args or args['message_service_synchronous'] is None:
            args['message_service_synchronous'] = not esgfpid.defaults.RABBIT_IS_ASYNCHRONOUS

        if 'consumer_solr_url' not in args or args['consumer_solr_url'] is None:
            args['consumer_solr_url'] = None

    def __check_rabbit_credentials_completeness(self, args):
        for credentials in args['messaging_service_credentials']:

            if not isinstance(credentials, dict):
                errmsg = 'Credentials for each RabbitMQ node should be a dictionary.'
                raise esgfpid.exceptions.ArgumentError(errmsg)

            # Mandatory:
            self.__check_presence_and_type('url', credentials, basestring)
            self.__check_presence_and_type('user', credentials, basestring)
            self.__check_presence_and_type('password', credentials, basestring) # If you want open nodes to be enabled again, remove this!
            
            # Optional:
            self.__check_and_adapt_type_if_exists('password', credentials, basestring)
            self.__check_and_adapt_type_if_exists('vhost', credentials, basestring)
            self.__check_and_adapt_type_if_exists('port', credentials, int)
            self.__check_and_adapt_type_if_exists('ssl_enabled', credentials, bool)

    def __check_presence_and_type(self, attname, credentials, desiredtype):
        self.__check_presence(attname, credentials)
        self.__check_and_adapt_type_if_exists(attname, credentials, desiredtype)

    def __check_presence(self, attname, credentials):
        if attname not in credentials:
            rabbitname_for_errmsg = '(not specified)'
            if 'url' in credentials:
                rabbitname_for_errmsg = credentials['url']
            errmsg = 'Missing %s for messaging service "%s"!' % (attname, rabbitname_for_errmsg)
            raise esgfpid.exceptions.ArgumentError(errmsg)

    def __check_and_adapt_type_if_exists(self, attname, credentials, desiredtype):
        if attname in credentials:

            # Empty string to None:
            if credentials[attname] == '':
                credentials[attname] = None

            # List to object:
            if type(credentials[attname]) == type([]) and len(credentials[attname]) == 1:
                credentials[attname] = credentials[attname][0]

            # Don't check if None:
            if credentials[attname] is None:
                pass

            # Check type:
            elif not isinstance(credentials[attname], desiredtype):

                # Try conversion:
                try:
                    credentials[attname] = self.__try_conversion(credentials[attname], desiredtype)

                except ValueError as e:
                    errmsg = ('Wrong type of messaging service %s (%s). Expected %s, got %s, conversion failed.' % 
                        (attname, credentials[attname], desiredtype, type(credentials[attname])))
                    raise esgfpid.exceptions.ArgumentError(errmsg)

    def __try_conversion(self, value, desiredtype):
        if desiredtype == bool:
            if isinstance(value, basestring):
                if str.lower(value) == 'true':
                    return True
                elif str.lower(value) == 'false':
                    return False
            raise ValueError()
        if desiredtype == basestring:
            #return str(value)
            raise ValueError('Not transforming booleans')
        if desiredtype == int:
            return int(value)
        else:
            return desiredtype(value)

    '''
    These are not (only) needed during initialisation, but
    (also) later on.
    '''
    def __store_some_args(self, args):
        self.prefix = args['handle_prefix']
        self.__thredds_service_path = args['thredds_service_path']
        self.__data_node = args['data_node'] # may be None, only needed for some assistants.
        self.__consumer_solr_url = args['consumer_solr_url'] # may be None

    def __throw_error_if_prefix_not_in_list(self):
        if self.prefix is None:
            raise esgfpid.exceptions.ArgumentError('Missing handle prefix!')
        if self.prefix not in esgfpid.defaults.ACCEPTED_PREFIXES:
            raise esgfpid.exceptions.ArgumentError('The prefix "%s" is not a valid prefix! Please check your config. Accepted prefixes: %s'
                % (self.prefix, ', '.join(esgfpid.defaults.ACCEPTED_PREFIXES)))

    def create_publication_assistant(self, **args):
        '''
        Create an assistant for a dataset that allows to make PID
        requests for the dataset and all of its files.

        :param drs_id: Mandatory. The dataset id of the dataset
            to be published.

        :param version_number: Mandatory. The version number of the
            dataset to be published.

        :param is_replica: Mandatory. Flag to indicate whether the
            dataset is a replica.

        .. note:: If the replica flag is set to False, the publication
            may still be considered a replica by the consuming servlet,
            namely if the dataset was already published at a different
            host. For this, please refer to the consumer documentation.

        :return: A publication assistant which provides all necessary
            methods to publish a dataset and its files.
        '''

        # Check args
        logdebug(LOGGER, 'Creating publication assistant..')
        mandatory_args = ['drs_id', 'version_number', 'is_replica']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        # Check if service path is given
        if self.__thredds_service_path is None:
            msg = 'No thredds_service_path given (but it is mandatory for publication)'
            logwarn(LOGGER, msg)
            raise esgfpid.exceptions.ArgumentError(msg)
        # Check if data node is given
        if self.__data_node is None:
            msg = 'No data_node given (but it is mandatory for publication)'
            logwarn(LOGGER, msg)
            raise esgfpid.exceptions.ArgumentError(msg)

        # Check if solr has access:
        if self.__coupler.is_solr_switched_off():
            pass # solr access not mandatory anymore

        # Create publication assistant
        assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(
            drs_id=args['drs_id'],
            version_number=args['version_number'],
            thredds_service_path=self.__thredds_service_path,
            data_node=self.__data_node,
            prefix=self.prefix,
            coupler=self.__coupler,
            is_replica=args['is_replica'],
            consumer_solr_url=self.__consumer_solr_url # may be None
        )
        logdebug(LOGGER, 'Creating publication assistant.. done')
        return assistant

    def unpublish_one_version(self, **args):
        '''
        Sends a PID update request for the unpublication of one version
        of a dataset currently published at the given data node.

        Either the handle or the pair of drs_id and version_number
        have to be provided, otherwise an exception will occur.

        The consumer will of course check the PID request message's
        timestamp with the timestamp of the last publication, so that
        republications in the mean time are not unpublished.

        The unpublication of the files is included in this method.

        :param handle: Optional. The handle of the dataset
            to be unpublished.

        :param drs_id: Optional. The dataset id of the dataset
            to be unpublished.

        :param version_number: Optional. The version number of
            the dataset to be unpublished.

        :raises: ArgumentError: If not enough arguments are passed
            to identify the dataset, or if no data node was specified
            during library init.

        '''

        # Check args
        optional_args = ['handle', 'drs_id', 'version_number']
        esgfpid.utils.add_missing_optional_args_with_value_none(args, optional_args)

        # Check if data node is given
        if self.__data_node is None:
            msg = 'No data_node given (but it is mandatory for unpublication)'
            logwarn(LOGGER, msg)
            raise esgfpid.exceptions.ArgumentError(msg)

        # Unpublish
        assistant = esgfpid.assistant.unpublish.AssistantOneVersion(
            drs_id = args['drs_id'],
            data_node = self.__data_node,
            prefix=self.prefix,
            coupler=self.__coupler,
            message_timestamp=esgfpid.utils.get_now_utc_as_formatted_string()
        )
        assistant.unpublish_one_dataset_version(
            handle = args['handle'],
            version_number = args['version_number']
        )
 
    def unpublish_all_versions(self, **args):
        '''
        Sends a PID update request for the unpublication of all versions
        of a dataset currently published at the given data node.

        If the library has solr access, it will try to find all the
        dataset versions and their handles from solr, and send individual
        messages for each version. Otherwise, one message is sent, and the
        queue consuming servlet has to identify the relevant versions,
        also making sure not to unpublish any versions that may have been
        republished in the meantime.

        :param drs_id: Dataset id of the dataset to be unpublished.
        :raises: ArgumentError: If the data node
                 was not provided at library initialization.

        '''

        # Check args
        mandatory_args = ['drs_id']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

        # Check if data node is given
        if self.__data_node is None:
            msg = 'No data_node given (but it is mandatory for publication)'
            logwarn(LOGGER, msg)
            raise esgfpid.exceptions.ArgumentError(msg)

        # Check if solr has access:
        if self.__coupler.is_solr_switched_off():
            msg = 'Unpublication of all versions. Without solr access, we cannot identify the versions, so the consumer will have to take care of this.'
            logdebug(LOGGER, msg)
            #raise esgfpid.exceptions.ArgumentError('No solr access. Solr access is needed for publication. Please provide access to a solr index when initializing the library')

        # Unpublish
        assistant = esgfpid.assistant.unpublish.AssistantAllVersions(
            drs_id = args['drs_id'],
            data_node = self.__data_node,
            prefix=self.prefix,
            coupler=self.__coupler,
            message_timestamp=esgfpid.utils.get_now_utc_as_formatted_string(),
            consumer_solr_url = self.__consumer_solr_url # may be None
        )
        assistant.unpublish_all_dataset_versions()

    def add_errata_ids(self, **args):
        '''
        Add errata ids to a dataset handle record.

        To call this method, you do not need to provide the
        PID of the dataset. Instead, the PID string is derived
        from the dataset id and the version number.

        :param errata_ids: Mandatory. A list of errata ids (strings)
            to be added to the handle record.

        :param drs_id: Mandatory. The dataset id of the dataset
            to whose handle record the errata ids are to be
            added. (This is needed because the handle is found
            by making a hash over dataset id and version number).

        :param version_number: Mandatory. The version number of the
            dataset to whose handle record the errata ids are to be
            added. (This is needed because the handle is found by
            making a hash over dataset id and version number).
        '''

        # Check args:
        mandatory_args = ['drs_id', 'version_number', 'errata_ids']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)

        # Perform metadata update
        assistant = esgfpid.assistant.errata.ErrataAssistant(
            coupler=self.__coupler,
            prefix=self.prefix
        )
        assistant.add_errata_ids(
            drs_id=args['drs_id'],
            version_number=args['version_number'],
            errata_ids=args['errata_ids']
        )

    def remove_errata_ids(self, **args):
        '''
        Remove errata ids from a dataset handle record.

        To call this method, you do not need to provide the
        PID of the dataset. Instead, the PID string is derived
        from the dataset id and the version number.

        :param errata_ids: Mandatory. A list of errata ids (strings) to
            be removed from the handle record.

        :param drs_id: Mandatory. The dataset id of the dataset
            from whose handle record the errata ids are to be
            removed. (This is needed because the handle is found
            by making a hash over dataset id and version number).

        :param version_number: Mandatory. The version number of the
            dataset from whose handle record the errata ids are to be
            removed. (This is needed because the handle is found by
            making a hash over dataset id and version number).

        '''

        # Check args:
        mandatory_args = ['drs_id', 'version_number', 'errata_ids']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)

        # Perform metadata update
        assistant = esgfpid.assistant.errata.ErrataAssistant(
            coupler=self.__coupler,
            prefix=self.prefix
        )
        assistant.remove_errata_ids(
            drs_id=args['drs_id'],
            version_number=args['version_number'],
            errata_ids=args['errata_ids']
        )

    def create_data_cart_pid(self, dict_of_drs_ids_and_pids):
        '''
        Create a handle record for a data cart (a custom set of datasets).

        The handle string is made of the prefix passed to tbe library,
        and a hash over all the dataset ids in the cart. This way, if exactly
        the same set of datasets is passed several times, the same handle
        record is created, instead of making a new one.

        :param dict_of_drs_ids_and_pids: A dictionary of all dataset ids
            and their pid strings. If a dataset has no (known) PID, use
            "None".

        :return: The handle string for this data cart.
        '''
        assistant = esgfpid.assistant.datacart.DataCartAssistant(
            prefix=self.prefix,
            coupler=self.__coupler
        )
        return assistant.make_data_cart_pid(dict_of_drs_ids_and_pids)

    def start_messaging_thread(self):
        '''
        Start the parallel thread that takes care of the asynchronous
        communication with RabbitMQ.

        If PID creation/update requests are attempted before
        this was called, an exception will be raised.

        Preferably call this method as early as possible, so that
        the module has some time to build the connection before
        the first PID requests are made.
        (If PID requests are made before the connection is ready,
        they will not be lost, but pile up and sent once the connection
        is ready).

        .. important:: Please do not forget to finish the thread at the end,
            using :meth:`~esgfpid.connector.Connector.finish_messaging_thread`
            or :meth:`~esgfpid.connector.Connector.force_finish_messaging_thread`.

        '''
        self.__coupler.start_rabbit_connection()

    def finish_messaging_thread(self):
        '''
        Finish and join the parallel thread that takes care of
        the asynchronous communication with RabbitMQ.

        If some messages are still in the stack to be sent,
        or if some messages were not confirmed yet, this method
        blocks and waits for some time while it iteratively
        checks for message confirmation.

        Currently, it waits up to 5 seconds: It checks up to
        11 times, waiting 0.5 seconds in between - these
        values can be configured in the defaults module).
        '''
        self.__coupler.finish_rabbit_connection()

    def force_finish_messaging_thread(self):
        '''
        Finish and join the parallel thread that takes care of
        the asynchronous communication with RabbitMQ.

        This method does not wait for any pending messages.
        Messages that are not sent yet are lost. Messages that
        are not confirmed yet are probably not lost, but their
        receival is not guaranteed.

        Note:
        The rabbit module keeps a copy of all unsent and
        unconfirmed messages, so they could be resent in
        a later connection. It would also be easy to expose
        a method for the library caller to retrieve those
        messages, e.g. to write them into some file.


        '''
        self.__coupler.force_finish_rabbit_connection()

    def make_handle_from_drsid_and_versionnumber(self, **args):
        '''
        Create a handle string for a specific dataset, based
        on its dataset id and version number, and the prefix
        passed to the library at initializing.

        :param drs_id: The dataset id of the dataset.
        :param version_number: The version number of the dataset
            (as a string or integer, this does not matter)
        :return: A handle string (e.g. "hdl:21.14100/abcxyzfoo")
        '''
        args['prefix'] = self.prefix
        return esgfpid.utils.make_handle_from_drsid_and_versionnumber(**args)
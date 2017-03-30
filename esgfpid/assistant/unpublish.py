import logging
import esgfpid.utils
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())
        
class AssistantOneVersion(object):
 
    def __init__(self, **args):
        mandatory_args = ['drs_id', 'data_node', 'prefix', 'coupler']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

        self._coupler = args['coupler']
        self._drs_id = args['drs_id']
        self._data_node = args['data_node'].rstrip('/')
        self._prefix = args['prefix']

    def unpublish_one_dataset_version(self, **args):
        optional_args = ['dataset_handle', 'version_number']
        esgfpid.utils.add_missing_optional_args_with_value_none(args, optional_args)

        handle = args['dataset_handle']
        version_number = args['version_number']

        if handle and version_number:
            self.__both_given(handle, version_number)
            loginfo(LOGGER, 'Requesting to unpublish version %s of dataset %s from %s (handle: %s).', version_number, self._drs_id, self._data_node, handle)
        elif handle:
            self.__only_handle_given(handle)
            loginfo(LOGGER, 'Requesting to unpublish a version of dataset %s from %s (handle: %s).', self._drs_id, self._data_node, handle)
        elif version_number:
            self.__only_version_given(version_number)
            loginfo(LOGGER, 'Requesting to unpublish version %s of dataset %s from %s.', version_number, self._drs_id, self._data_node)
        else:
            msg = 'Neither a handle nor a version number were specified for unpublication!'
            raise esgfpid.exceptions.ArgumentError(msg)

    def __both_given(self, handle, version_number):
        self.__check_whether_handle_and_version_number_match(handle, version_number)
        logdebug(LOGGER, 'Unpublishing dataset with handle %s (one version, %s).', handle, version_number)
        message = self.__make_message(handle, version_number=version_number)
        self._send_message_to_queue(message)

    def __only_handle_given(self, handle):
        logdebug(LOGGER, 'Unpublishing dataset with handle %s (one version).', handle)
        message = self.__make_message(handle)
        self._send_message_to_queue(message)

    def __only_version_given(self, version_number):
        handle = self.__derive_handle_from_version(version_number)
        logdebug(LOGGER, 'Unpublishing dataset with handle %s (one version, %s). Handle was derived from version number', handle, version_number)
        message = self.__make_message(handle, version_number=version_number)
        self._send_message_to_queue(message)

    def __derive_handle_from_version(self, version_number):
        derived_handle = esgfpid.utils.make_handle_from_drsid_and_versionnumber(
            drs_id=self._drs_id,
            version_number=version_number,
            prefix=self._prefix
        )
        return derived_handle

    def __check_whether_handle_and_version_number_match(self, specified_handle, version_number):
        derived_handle = self.__derive_handle_from_version(version_number)
        if derived_handle != specified_handle:
            msg = ('Given handle not the same as computed handle. '+
                   'Given: '+specified_handle+', '+
                   'computed: '+derived_handle+
                   ' (from '+self._drs_id+' and '+str(version_number)+').'
            )
            LOGGER.error(msg)
            raise ValueError(msg)
            # TODO COMPLAIN LOUDLY

    def __make_message(self, handle, **kwargs):
        message_timestamp = esgfpid.utils.get_now_utc_as_formatted_string()

        message = esgfpid.assistant.messages.unpublish_one_version(
            timestamp = message_timestamp,
            data_node = self._data_node,
            dataset_handle = handle,
            drs_id = self._drs_id
        )
        if 'version_number' in kwargs:
            message['version_number'] = int(kwargs['version_number'])
        return message

    def _send_message_to_queue(self, message):
        self._coupler.send_message_to_queue(message)

class AssistantAllVersions(AssistantOneVersion):

    def __init__(self, **args):
        super(AssistantAllVersions, self).__init__(**args)
        if 'consumer_solr_url' not in args.keys():
            args['consumer_solr_url'] = None
        self.__consumer_solr_url = args['consumer_solr_url']

    def unpublish_all_dataset_versions(self):

        # If solr is switched off, consumer must find versions:
        if self._coupler.is_solr_switched_off():
            self.__unpublish_allversions_consumer_must_find_versions()

        # Get handles or version numbers from solr:
        else:
            all_handles_or_versionnumbers = self.__get_all_handles_or_versionnumbers()
            all_handles = all_handles_or_versionnumbers['dataset_handles']
            all_version_numbers = all_handles_or_versionnumbers['version_numbers']

            # If we can have all versions' handles, it's easy.
            if all_handles is not None:
                self.__unpublish_all_dataset_versions_by_handle(all_handles)

            # If not, we have the version numbers (and can make the handles from them):
            elif all_version_numbers is not None:
                self.__unpublish_all_dataset_versions_by_version(all_version_numbers)

            # If neither, let the consumer find them
            else:
                self.__unpublish_allversions_consumer_must_find_versions()
                loginfo(LOGGER, 'Requesting to unpublish all versions of dataset %s from %s', self._drs_id, self._data_node)

    def __unpublish_all_dataset_versions_by_version(self, all_version_numbers):
        for version_number in all_version_numbers:
            self.unpublish_one_dataset_version(version_number=version_number)
  
    def __unpublish_all_dataset_versions_by_handle(self, all_handles):
        for handle in all_handles:
            self.unpublish_one_dataset_version(dataset_handle=handle)

    def __unpublish_allversions_consumer_must_find_versions(self):
        message = self.__make_message_case_consumer_must_find_versions()
        self._send_message_to_queue(message)

    def __make_message_case_consumer_must_find_versions(self):
        message_timestamp = esgfpid.utils.get_now_utc_as_formatted_string()

        message = esgfpid.assistant.messages.unpublish_allversions_consumer_must_find_versions(
            timestamp=message_timestamp,
            drs_id=self._drs_id,
            data_node=self._data_node,
            consumer_solr_url=self.__consumer_solr_url)
        return message

    def __get_all_handles_or_versionnumbers(self):
        all_handles_or_versionnumbers = self._coupler.retrieve_datasethandles_or_versionnumbers_of_allversions(
            drs_id=self._drs_id
        )
        return all_handles_or_versionnumbers


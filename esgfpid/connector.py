''' This module represents the API of the ESGF PID module.'''

import logging
import esgfpid.assistant.publish
import esgfpid.assistant.unpublish
import esgfpid.assistant.errata
import esgfpid.assistant.shoppingcart
import esgfpid.exceptions
import esgfpid.coupling
import esgfpid.utils

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class Connector(object):

    def __init__(self, **args):
        LOGGER.debug(40*'-')
        LOGGER.debug('Creating PID connector object ..')
        self.__check_presence_of_args(args)
        self.__store_some_args(args)
        self.__coupler = esgfpid.coupling.Coupler(**args)
        LOGGER.debug('Creating PID connector object .. done')

    def __check_presence_of_args(self, args):
        mandatory_args = [
            'messaging_service_urls',
            'messaging_service_exchange_name',
            'messaging_service_username',
            'handle_prefix',
            'data_node'
        ]
        optional_args = [
            'solr_url',
            'messaging_service_url_preferred',
            'messaging_service_password',
            'solr_https_verify',
            'disable_insecure_request_warning',
            'solr_switched_off',
            'thredds_service_path',
            'consumer_solr_url',
            'test_publication'
        ]
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.add_missing_optional_args_with_value_none(args, optional_args)

    def __store_some_args(self, args):
        self.prefix = args['handle_prefix']
        self.__thredds_service_path = args['thredds_service_path']
        self.__data_node = args['data_node']
        self.__consumer_solr_url = args['consumer_solr_url'] # may be None

    def create_publication_assistant(self, **args):

        # Check args
        LOGGER.debug('Creating publication assistant..')
        mandatory_args = ['drs_id', 'version_number', 'is_replica']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

        # Check if solr has access:
        if self.__coupler.is_solr_switched_off():
            pass

        # Check if service path is given
        # (Not mandatory in connector, as it is not needed
        # for unpublication)
        if self.__thredds_service_path is None:
            msg = 'No thredds_service_path given (but it is mandatory for publication)'
            LOGGER.warn(msg)
            raise esgfpid.exceptions.ArgumentError(msg)

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
        LOGGER.debug('Creating publication assistant.. done')
        return assistant

    def unpublish_one_version(self, **args):

        # Check args
        optional_args = ['handle', 'drs_id', 'version_number']
        esgfpid.utils.add_missing_optional_args_with_value_none(args, optional_args)

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

        # Check args
        mandatory_args = ['drs_id']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

        # Check if solr has access:
        if self.__coupler.is_solr_switched_off():
            pass
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

    def create_shopping_cart_pid(self, list_of_handles_or_drs_strings):
        assistant = esgfpid.assistant.shoppingcart.ShoppingCartAssistant(
            prefix=self.prefix,
            coupler=self.__coupler
        )
        return assistant.make_shopping_cart_pid(list_of_handles_or_drs_strings)

    def start_messaging_thread(self):
        self.__coupler.start_rabbit_connection()

    def finish_messaging_thread(self):
        self.__coupler.finish_rabbit_connection()

    def force_finish_messaging_thread(self):
        self.__coupler.force_finish_rabbit_connection()

    def make_handle_from_drsid_and_versionnumber(self, **args):
        args['prefix'] = self.prefix
        return esgfpid.utils.make_handle_from_drsid_and_versionnumber(**args)
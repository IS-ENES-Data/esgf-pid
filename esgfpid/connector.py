'''
This module represents the API of the ESGF PID module.

Author: Merret Buurman (DKRZ), 2015-2016

'''

import logging
import esgfpid.assistant.publish
import esgfpid.assistant.unpublish
import esgfpid.assistant.errata
import esgfpid.assistant.shoppingcart
import esgfpid.defaults
import esgfpid.exceptions
import esgfpid.coupling
import esgfpid.utils
from esgfpid.utils import loginfo, logdebug, logwarn

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class Connector(object):

    def __init__(self, **args):
        LOGGER.debug(40*'-')
        LOGGER.debug('Creating PID connector object ..')
        self.__check_presence_of_args(args)
        self.__store_some_args(args)
        self.__throw_error_if_prefix_not_in_list()
        self.__coupler = esgfpid.coupling.Coupler(**args)
        loginfo(LOGGER, 'Created PID connector.')

    def __check_presence_of_args(self, args):
        mandatory_args = [
            'messaging_service_urls',
            'messaging_service_exchange_name',
            'messaging_service_username',
            'handle_prefix'
        ]
        optional_args = [
            'solr_url',
            'data_node',
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
        self.__data_node = args['data_node'] # may be None, only needed for some assistants.
        self.__consumer_solr_url = args['consumer_solr_url'] # may be None

    def __throw_error_if_prefix_not_in_list(self):
        if self.prefix is None:
            raise esgfpid.exceptions.ArgumentError('Prefix not set yet, cannot check its existence.')
        if self.prefix not in esgfpid.defaults.ACCEPTED_PREFIXES:
            raise esgfpid.exceptions.ArgumentError('The prefix "%s" is not a valid prefix! Please check your config. Accepted prefixes: %s'
                % (self.prefix, ', '.join(esgfpid.defaults.ACCEPTED_PREFIXES)))

    def create_publication_assistant(self, **args):

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

        # Check args
        optional_args = ['handle', 'drs_id', 'version_number']
        esgfpid.utils.add_missing_optional_args_with_value_none(args, optional_args)

        # Check if data node is given
        if self.__data_node is None:
            msg = 'No data_node given (but it is mandatory for publication)'
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

    def remove_errata_ids(self, **args):

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
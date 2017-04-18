import logging
import esgfpid.exceptions
from . import utils as solrutils

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class FindVersionsOfSameDataset(object):

    def __init__(self, solr_interactor):
        self.__solr_interactor = solr_interactor
        self.__error_messages = None

    def __reset_error_messages(self):
        self.__error_messages = []

    # General methods:

    def retrieve_dataset_handles_or_version_numbers_of_all_versions(self, drs_id, prefix):
        '''
        :return: Dict. Should never return None.
        :raise: SolrSwitchedOff
        :raise SolrError: If ...
        '''
        self.__reset_error_messages()
        response_json = self.__ask_solr_for_handles_or_version_numbers_of_all_versions(drs_id)
        result_dict = self.__parse_result_handles_or_version_numbers_of_all_versions(response_json, prefix)
        return result_dict

    # Querying solr for handles and/or version numbers of all versions of a dataset (same drs_id):

    def __ask_solr_for_handles_or_version_numbers_of_all_versions(self, drs_id):
        LOGGER.debug('Asking solr for dataset handles or version numbers of all dataset versions with the drs_id "%s".', drs_id)
        query = self.__make_query_for_handles_or_version_numbers_of_all_versions(drs_id)
        LOGGER.debug('Query: %s', query)
        response_json = self.__solr_interactor.send_query(query) # can raise SolrError or SolrSwitchedOff, but can't be None
        return response_json

    def __make_query_for_handles_or_version_numbers_of_all_versions(self, drs_id):
        query_dict = self.__solr_interactor.make_solr_base_query()
        query_dict['type'] = 'Dataset'
        query_dict['facets'] = 'pid,version'
        query_dict['drs_id'] = drs_id
        return query_dict

    def __parse_result_handles_or_version_numbers_of_all_versions(self, response_json, prefix):

        # Prepare result object:
        result_dict = {}
        result_dict['dataset_handles'] = None
        result_dict['version_numbers'] = None

        # Get handles, if there is any:
        result_dict['dataset_handles'] = self.__get_handles_if_any(response_json, prefix)

        # Otherwise, get version numbers, if there is any:
        result_dict['version_numbers'] = self.__get_version_numbers_if_any(response_json)

        if (result_dict['dataset_handles'] is None and
            result_dict['version_numbers'] is None):

            msg = 'Found neither version numbers, nor handles. Errors: %s' % '; '.join(self.__error_messages)
            LOGGER.debug(msg)
            raise esgfpid.exceptions.SolrResponseError(msg)

        return result_dict

    def __get_handles_if_any(self, response_json, prefix):
        try:
            dataset_handles = solrutils.extract_dataset_handles_from_response_json(response_json, prefix)
            if len(dataset_handles) > 0:
                LOGGER.debug('Found dataset handles: %s', dataset_handles)
            return dataset_handles

        except esgfpid.exceptions.SolrResponseError as e:
            self.__error_messages.append(e.message)

    def __get_version_numbers_if_any(self, response_json):
        try:
            version_numbers = solrutils.extract_dataset_version_numbers_from_response_json(response_json)
            if len(version_numbers) > 0:
                LOGGER.debug('Found version numbers, but no handles: %s', version_numbers)
            return version_numbers

        except esgfpid.exceptions.SolrResponseError as e:
            self.__error_messages.append(e.message)
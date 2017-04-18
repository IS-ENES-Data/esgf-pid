import esgfpid.utils
import esgfpid.exceptions
import logging
from . import utils as solrutils


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class FindFilesOfSameDatasetVersion(object):

    def __init__(self, solr_interactor):
        self.__solr_interactor = solr_interactor
        self.__error_messages = None

    def __reset_error_messages(self):
        self.__error_messages = []

    # General methods:

    def retrieve_file_handles_of_same_dataset(self, **args):
        '''
        :return: List of handles, or empty list. Should never return None.
        :raise: SolrSwitchedOff
        :raise SolrError: If both strategies to find file handles failed.
        '''
        mandatory_args = ['drs_id', 'version_number', 'data_node', 'prefix']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        self.__reset_error_messages()

        # Try plan A
        file_handles = None
        try:
            file_handles = self.__strategy1(args) # can raise SolrError or SolrSwitchedOff, but can't return None
        except esgfpid.exceptions.SolrError as e:
            self.__error_messages.append('Error during first query: '+e.message)

        if file_handles is not None and len(file_handles)>0:
            LOGGER.debug('Retrieved file handles from solr in first query.')
            return file_handles

        # Try plan B
        try:
            file_handles = self.__strategy2(args) # can raise SolrError or SolrSwitchedOff, but can't return None
        except esgfpid.exceptions.SolrError as e:
            self.__error_messages.append('Error during second query: '+e.message)
            msg = '/n'.join(self.__error_messages)
            raise esgfpid.exceptions.SolrError('Failure in both queries. Messages:\n'+msg)

        return file_handles

    def __strategy1(self, args):
        return self.__retrieve_file_handles_of_same_dataset_if_same_datanode(
            args['drs_id'],
            args['version_number'],
            args['data_node'],
            args['prefix'])

    def __strategy2(self, args):
        return self.__retrieve_file_handles_of_same_dataset_if_different_datanode(
            args['drs_id'],
            args['version_number'],
            args['prefix'])

    def __retrieve_file_handles_of_same_dataset_if_same_datanode(self, drs_id, version_number, data_node, prefix):
        dataset_id = self.__make_dataset_id_from_drsid_and_versnum(drs_id, version_number, data_node)
        response_json = self.__ask_solr_for_handles_of_files_with_same_dataset_id(dataset_id) # can raise SolrError or SolrSwitchedOff, but can't be None
        file_handles = solrutils.extract_file_handles_from_response_json(response_json, prefix) # can raise SolrReponseError
        if len(file_handles)==0:
            msg = 'First query returned an empty list.'
            LOGGER.debug(msg)
            self.__error_messages.append(msg)
        return file_handles

    def __make_dataset_id_from_drsid_and_versnum(self, drs_id, version_number, data_node):
        dataset_id = drs_id + '.v' + str(version_number) + '|' + data_node
        return dataset_id

    def __retrieve_file_handles_of_same_dataset_if_different_datanode(self, drs_id, version_number, prefix):
        response_json = self.__ask_solr_for_handles_of_files_with_same_drsid_and_version(drs_id, version_number) # can raise SolrError or SolrSwitchedOff, but can't be None
        file_handles = solrutils.extract_file_handles_from_response_json(response_json, prefix) # can raise SolrReponseError
        if len(file_handles)==0:
            self.__error_messages.append('Second query returned an empty list.')
        return file_handles

    # Querying solr for handles of files with the same dataset id:

    def __ask_solr_for_handles_of_files_with_same_dataset_id(self, dataset_id):
        LOGGER.debug('Asking solr for all handles or files with the dataset_id "%s".', dataset_id)
        query = self.__make_query_handles_of_files_with_same_dataset_id(dataset_id)
        LOGGER.debug('Query: %s', query)
        response_json = self.__solr_interactor.send_query(query) # can raise SolrError or SolrSwitchedOff, but can't be None
        return response_json

    def __make_query_handles_of_files_with_same_dataset_id(self, dataset_id):
        query_dict = self.__solr_interactor.make_solr_base_query()
        query_dict['type'] = 'File'
        query_dict['facets'] = 'tracking_id'
        query_dict['dataset_id'] = dataset_id
        return query_dict

    # Querying solr for the dataset_ids of all versions with the same drs_id:

    def __ask_solr_for_handles_of_files_with_same_drsid_and_version(self, drs_id, version_number):
        part_of_dataset_id = self.__make_half_dataset_id_from_drsid_and_versnum(drs_id, version_number)
        LOGGER.debug('Asking solr for all handles or files with the "half dataset_id" "%s".', part_of_dataset_id)
        query = self.__make_query_for_handles_of_files_with_same_drsid_and_version(part_of_dataset_id)
        LOGGER.debug('Query: %s', query)
        response_json = self.__solr_interactor.send_query(query) # can raise SolrError or SolrSwitchedOff, but can't be None
        return response_json

    def __make_half_dataset_id_from_drsid_and_versnum(self, drs_id, version_number):
        part_of_dataset_id = drs_id + '.v' + str(version_number) + '|'
        return part_of_dataset_id

    def __make_query_for_handles_of_files_with_same_drsid_and_version(self, part_of_dataset_id):
        query_dict = self.__solr_interactor.make_solr_base_query()
        query_dict['type'] = 'File'
        query_dict['facets'] = 'tracking_id'
        query_dict['query'] = 'dataset_id:'+part_of_dataset_id+'*'
        return query_dict

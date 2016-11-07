'''
This module provides the solr facade for the rest of the library.

All requests to solr are addressed to this class.
It redirects the queries for the various tasks to the various submodules, and
redirects the actual interaction with the solr server to another specific
submodule.

'''

import logging
import requests
import json
import esgfpid.utils
import esgfpid.solr.tasks.filehandles_same_dataset
import esgfpid.solr.serverconnector
import esgfpid.defaults
import esgfpid.exceptions
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class SolrInteractor(object):

    def __init__(self, **args):

        if self.__should_be_switched_off(args):
            logdebug(LOGGER, 'Initializing solr module without access..')
            self.__init_without_access(args)
            logdebug(LOGGER, 'Initializing solr module without access.. done')

        else:
            logdebug(LOGGER, 'Initializing solr module..')
            self.__init_with_access(args)
            logdebug(LOGGER, 'Initializing solr module.. done')

    def __should_be_switched_off(self, args):
        if 'switched_off' in args.keys() and args['switched_off'] == True:
            return True
        else:
            return False

    def is_switched_off(self):
        return not self.__switched_on

    def __init_without_access(self, args):
        self.__switched_on = False
        self.__prefix = None
        self.__solr_server_connector = None

    def __init_with_access(self, args):
        self.__switched_on = True
        self.__check_presence_of_args(args)
        self.__prefix = args['prefix']
        self.__make_server_connector(args)

    def __check_presence_of_args(self, args):
        mandatory_args = ['solr_url', 'prefix']
        optional_args = ['https_verify', 'disable_insecure_request_warning', 'switched_off']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.add_missing_optional_args_with_value_none(args, optional_args)

    def __make_server_connector(self, args):
        self.__solr_server_connector = esgfpid.solr.serverconnector.SolrServerConnector(
            solr_url = args['solr_url'],
            https_verify = args ['https_verify'],
            disable_insecure_request_warning = args['disable_insecure_request_warning']
        )

    def send_query(self, query):
        ''' This method is called by the tasks. It is redirected to the submodule.'''
        if self.__switched_on:
            return self.__solr_server_connector.send_query(query)
        else:
            msg = 'Not sending query'
            LOGGER.debug(msg)
            raise esgfpid.exceptions.SolrSwitchedOff(msg)

    def make_solr_base_query(self):
        query_dict = {}
        query_dict['distrib'] = esgfpid.defaults.SOLR_QUERY_DISTRIB
        query_dict['format'] = 'application/solr+json'
        query_dict['limit'] = 0 # As we don't want all the details of the found files/datasets!
        return query_dict

    #####################
    ### Various tasks ###
    #####################

    def retrieve_file_handles_of_same_dataset(self, **args):
        '''
        :return: List of handles, or empty list. Should never return None.
        :raise: SolrSwitchedOff
        :raise SolrError: If both strategies to find file handles failed.
        '''

        mandatory_args = ['drs_id', 'version_number', 'data_node']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        LOGGER.debug('Looking for files of dataset "%s", version "%s".',
                     args['drs_id'], str(args['version_number']))

        if self.__switched_on:
            return self.__retrieve_file_handles_of_same_dataset(**args)
        else:
            msg = 'Cannot retrieve handles of files of the same dataset.'
            raise esgfpid.exceptions.SolrSwitchedOff(msg)

    def __retrieve_file_handles_of_same_dataset(self, **args):
        finder = esgfpid.solr.tasks.filehandles_same_dataset.FindFilesOfSameDatasetVersion(self)
        args['prefix'] = self.__prefix
        file_handles = finder.retrieve_file_handles_of_same_dataset(**args)
        return file_handles

    def retrieve_datasethandles_or_versionnumbers_of_allversions(self, drs_id):
        LOGGER.debug('Looking for dataset handles or version numbers of '+
                     'dataset "%s".', drs_id)

        if self.__switched_on:
            return self.__retrieve_datasethandles_or_versionnumbers_of_allversions(drs_id)
        else:
            msg = 'Cannot retrieve handles or version numbers of all versions of the dataset.'
            raise esgfpid.exceptions.SolrSwitchedOff(msg)

    def __retrieve_datasethandles_or_versionnumbers_of_allversions(self, drs_id):
        finder = esgfpid.solr.tasks.all_versions_of_dataset.FindVersionsOfSameDataset(self)
        result_dict = finder.retrieve_dataset_handles_or_version_numbers_of_all_versions(drs_id, self.__prefix)
        return result_dict





import logging
import requests
import json
import esgfpid.utils
import esgfpid.solr.tasks.filehandles_same_dataset
import esgfpid.solr.tasks.all_versions_of_dataset
import esgfpid.solr.serverconnector
import esgfpid.defaults
import esgfpid.exceptions
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
This class provides the solr facade for the rest of
the library.

All requests to solr are addressed to an instance of
this class.

It redirects the calls for the various tasks to the
responsible submodules, which create solr queries and
can parse results (:py:mod:`~esgfpid.solr.tasks`)

The actual interaction with solr server (sending of
a ready-made query, receiving the response) is handled 
by another class
(:py:class:`~esgfpid.solr.serverconnector.SolrServerConnector`).

'''
class SolrInteractor(object):

    # Constructor:

    '''
    :param switched_off: Mandatory. Boolean.
    :param prefix: Mandatory if not switched off.
    :param solr_url: Mandatory if not switched off.
    :param https_verify: Mandatory if not switched off.
    :param disable_insecure_request_warning: Mandatory if not switched off.
    '''
    def __init__(self, **args):

        mandatory_args = [
            'switched_off',
            'prefix',
            'solr_url',
            'https_verify',
            'disable_insecure_request_warning'
        ]
        esgfpid.utils.check_presence_of_mandatory_args(args, ['switched_off'])

        if args['switched_off'] == True:
            logdebug(LOGGER, 'Initializing solr module without access..')
            self.__init_without_access()
            logdebug(LOGGER, 'Initializing solr module without access.. done')

        else:
            esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
            logdebug(LOGGER, 'Initializing solr module..')
            self.__init_with_access(args)
            logdebug(LOGGER, 'Initializing solr module.. done')

    def __init_without_access(self):
        self.__switched_on = False
        self.__prefix = None
        self.__solr_server_connector = None

    def __init_with_access(self, args):
        self.__switched_on = True
        self.__check_presence_of_args(args)
        self.__prefix = args['prefix']
        self.__make_server_connector(args)

    def __check_presence_of_args(self, args):
        mandatory_args = ['solr_url', 'prefix', 'https_verify',
            'disable_insecure_request_warning', 'switched_off']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)

    def __make_server_connector(self, args):
        self.__solr_server_connector = esgfpid.solr.serverconnector.SolrServerConnector(
            solr_url = args['solr_url'],
            https_verify = args ['https_verify'],
            disable_insecure_request_warning = args['disable_insecure_request_warning']
        )

    # Getter

    '''
    State getter.

    :returns: True if the solr module is switched off, i.e.
        it either received a switch-off flag from the library
        or had no solr URL passed. False if not switched off.
    '''
    def is_switched_off(self):
        return not self.__switched_on

    # Methods called by tasks:

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

    # Task 1

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

    # Task 2

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





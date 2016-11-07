import logging
import esgfpid
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class Checker(object):

    def __init__(self, **args):

        mandatory_args = ['drs_id', 'data_node', 'version_number', 'coupler']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)

        # Needed to retrieve stuff from solr:
        self.__coupler = args['coupler']
        self.__drs_id = args['drs_id']
        self.__version_number = args['version_number']
        self.__data_node = args['data_node']

        # Other         
        self.__list_of_previous_files = None # from solr
        self.__will_run_check = True
        self.__message_why_not = 'No reason specified.'

        # Is solr switched off?
        if self.__coupler.is_solr_switched_off():
            self.__will_run_check = False
        else:
            # query solr for previous file handles:
            self.__data_consistency_check_preparation()


    def can_run_check(self):
        return self.__will_run_check

    ### Preparation ###

    def __data_consistency_check_preparation(self):
        logdebug(LOGGER, 'Asking solr for info for consistency check.')
        self.__retrieve_list_of_previous_files_from_solr()
        logdebug(LOGGER, 'Asking solr for info for consistency check... done.')
        self.__decide_whether_run_check_and_inform()

    def __retrieve_list_of_previous_files_from_solr(self):
        self.__list_of_previous_files = None
        try:
            self.__list_of_previous_files = self.__retrieve_from_solr()
        except (esgfpid.exceptions.SolrSwitchedOff, esgfpid.exceptions.SolrError) as e:
            self.__will_run_check = False
            self.__log_solr_error_occured(e)            

    def __retrieve_from_solr(self):
        files = self.__coupler.retrieve_file_handles_of_same_dataset(
            drs_id=self.__drs_id,
            version_number=self.__version_number,
            data_node=self.__data_node
        )
        return files

    def __decide_whether_run_check_and_inform(self):
        if self.__list_of_previous_files is not None:
            
            if len(self.__list_of_previous_files) > 0:
                self.__will_run_check = True
                self.__log_previously_stored_files_found()

            else:
                self.__will_run_check = False
                self.__log_previously_stored_was_empty()
                
        else:
            self.__will_run_check = False
            self.__log_no_useful_info()

    def __log_solr_error_occured(self, exception):
        msg = ('No dataset integrity test could be run. Message: '+exception.message)
        self.__message_why_not = msg
        loginfo(LOGGER, msg)

    def __log_previously_stored_files_found(self):
        concat_files = ', '.join(self.__list_of_previous_files)
        type_files = type(self.__list_of_previous_files)
        logdebug(LOGGER, 'Previously published fileset: %s (%s)', concat_files, type_files)
        loginfo(LOGGER, 'Data integrity check will be run after files were specified.')

    def __log_previously_stored_was_empty(self):
        msg = ('Data integrity check: No previous information stored '+
               '(apparently this is a first publication).')
        self.__message_why_not = msg
        loginfo(LOGGER, msg)

    
    def __log_no_useful_info(self):
        msg = ('Solr did not return any useful information, '+
               'no dataset integrity test could be run '+
               '(in case this was not a first publication).')
        self.__message_why_not = msg
        loginfo(LOGGER, msg)


    ### Perform the check ###

    def data_consistency_check(self, list_of_given_files):
        if self.__will_run_check:

            if list_of_given_files is None or len(list_of_given_files)==0:
                raise ValueError('Please provide a list of file handles for the consistency check.')

            return self.__data_consistency_check(list_of_given_files)
        else:
            raise ValueError('Consistency check can not be run. Reason: %s' % self.__message_why_not)

    def __data_consistency_check(self, list_of_given_files):
        logdebug(LOGGER, 'Performing consistency check...')
        list_too_many_files = self.__check_for_superfluous_files(list_of_given_files)
        list_missing_files = self.__check_for_missing_files(list_of_given_files)

        any_inconsistency = len(list_missing_files) > 0 or len(list_too_many_files) > 0
        
        if any_inconsistency:
            self.__log_missing_files_if_any(list_missing_files)
            self.__log_too_many_files_if_any(list_too_many_files)
            logdebug(LOGGER, 'Performing consistency check... done (fail)')
            return False
        else:
            logdebug(LOGGER, 'Performing consistency check... done (success)')
            return True

    def __check_for_superfluous_files(self, list_of_given_files):
        list_too_many_files = []
        for file_handle in list_of_given_files:
            if file_handle not in self.__list_of_previous_files:
                list_too_many_files.append(file_handle)
        return list_too_many_files

    def __check_for_missing_files(self, list_of_given_files):
        list_missing_files = []
        for file_handle in self.__list_of_previous_files:
            if file_handle not in list_of_given_files:
                list_missing_files.append(file_handle)
        return list_missing_files

    def __log_too_many_files_if_any(self, list_too_many_files):
        if len(list_too_many_files)>0:
            list_string = ','.join(list_too_many_files)
            LOGGER.warning('Some superfluous files were added in republication of dataset: %s',
                list_string)

    def __log_missing_files_if_any(self, list_missing_files):
        if len(list_missing_files)>0:
            list_string = ','.join(list_missing_files)
            LOGGER.warning(
                'Some files were missing in republication of dataset: %s',
                list_string)

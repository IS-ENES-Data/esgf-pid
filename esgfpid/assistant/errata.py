import logging
import uuid
import json
import esgfpid.utils
import esgfpid.assistant.messages
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class ErrataAssistant(object):

    def __init__(self, **args):
        '''
        :param coupler: The coupler object (for sending the message to the queue).
        '''
        mandatory_args = ['prefix', 'coupler']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)

        self.__prefix = args['prefix']
        self.__coupler = args['coupler']

    def add_errata_ids(self, **args):
        logdebug(LOGGER, 'Adding errata ids...')
        mandatory_args = ['drs_id', 'version_number', 'errata_ids']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)

        dataset_handle = self.__get_dataset_handle(args)
        errata_ids = self.__get_errata_ids_as_list(args)
        message = self.__make_add_message(errata_ids, dataset_handle, args['drs_id'], args['version_number'])
        self.__send_message_to_queue(message)

        loginfo(LOGGER, 'Requesting to add errata ids "%s" to dataset "%s".', ', '.join(errata_ids), dataset_handle)
        logdebug(LOGGER, 'Adding errata ids... done')


    def remove_errata_ids(self, **args):
        logdebug(LOGGER, 'Removing errata ids...')
        mandatory_args = ['drs_id', 'version_number', 'errata_ids']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)

        dataset_handle = self.__get_dataset_handle(args)
        errata_ids = self.__get_errata_ids_as_list(args)
        message = self.__make_remove_message(errata_ids, dataset_handle, args['drs_id'], args['version_number'])
        self.__send_message_to_queue(message)

        loginfo(LOGGER, 'Requesting to remove errata ids "%s" from dataset "%s".', ', '.join(errata_ids), dataset_handle)
        logdebug(LOGGER, 'Removing errata ids... done')

    def __get_errata_ids_as_list(self, args):
        errata_ids = args['errata_ids']
        if type(errata_ids) == type([]):
            return errata_ids
        else:
            return [errata_ids]

    def __get_dataset_handle(self, args):
        dataset_handle = esgfpid.utils.make_handle_from_drsid_and_versionnumber(
            drs_id=args['drs_id'],
            version_number=args['version_number'],
            prefix=self.__prefix
        )
        return dataset_handle

    def __make_add_message(self, errata_ids, dataset_handle, drs_id, vers_number):
        message_timestamp = esgfpid.utils.get_now_utc_as_formatted_string()
        
        message = esgfpid.assistant.messages.add_errata_ids_message(
            dataset_handle = dataset_handle,
            timestamp = message_timestamp,
            errata_ids = errata_ids,
            drs_id=drs_id,
            version_number=vers_number
        )
        return message

    def __make_remove_message(self, errata_ids, dataset_handle, drs_id, vers_number):
        message_timestamp = esgfpid.utils.get_now_utc_as_formatted_string()
        
        message = esgfpid.assistant.messages.remove_errata_ids_message(
            dataset_handle = dataset_handle,
            timestamp = message_timestamp,
            errata_ids = errata_ids,
            drs_id=drs_id,
            version_number=vers_number
        )
        return message


    def __send_message_to_queue(self, message):
        self.__coupler.send_message_to_queue(message)
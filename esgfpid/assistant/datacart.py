import logging
import esgfpid.utils
import esgfpid.assistant.messages
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
This class is responsible for creating data cart handles.

Its API consists of one method, make_data_cart_pid().

Dependencies:
 - esgfpid.coupling.Coupler object, to send the JSON message to RabbitMQ.
 - esgfpid.assistant.messages module to create the JSON message.
 - esgfpid.utils module to create the handle string.
'''
class DataCartAssistant(object):

    '''
    Constructor for a data cart assistant.

    :param prefix:  The handle prefix, used for creating a handle
                    string for this data cart.
    :param coupler: The coupler object, used to send the message
                    to RabbitMQ.
    '''
    def __init__(self, **args):
        mandatory_args = ['prefix', 'coupler']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)
        self.__prefix = args['prefix']
        self.__coupler = args['coupler']

    '''
    Create a handle string for a data cart, based
    on its content, so that with the same content,
    the handle string is always the same.

    :param dict_of_drs_ids_and_pids:
    :param prefix:
    '''
    @staticmethod
    def _get_handle_string_for_datacart(dict_of_drs_ids_and_pids, prefix):
        list_of_drs_ids = dict_of_drs_ids_and_pids.keys()
        hash_basis = esgfpid.utils.make_sorted_lowercase_list_without_hdl(list_of_drs_ids)
        return esgfpid.utils.make_handle_from_list_of_strings(hash_basis, prefix, addition='datacart')
        # This sorts the list, removes all "hdl:", and makes a hash
    
    '''
    Create a request to make a handle for a data cart, and send 
    it to the RabbitMQ.

    :param dict_of_drs_ids_and_pids: A dictionary of all the 
            dataset ids and dataset pids that should be contained
            in this 
    '''
    def make_data_cart_pid(self, dict_of_drs_ids_and_pids):
        logdebug(LOGGER, 'Making a PID for a data cart full of datasets...')

        # Check arg
        if not type(dict_of_drs_ids_and_pids) == type(dict()):
            if type(dict_of_drs_ids_and_pids) == type([]):
                raise esgfpid.exceptions.ArgumentError('Please provide a dictionary of dataset ids and handles, not a list')
            else:
                raise esgfpid.exceptions.ArgumentError('Please provide a dictionary of dataset ids and handles')

        # Make a pid (hash on the content):
        cart_handle = DataCartAssistant._get_handle_string_for_datacart(
            dict_of_drs_ids_and_pids,
            self.__prefix
        )

        # Make and send message
        message = self.__make_message(cart_handle, dict_of_drs_ids_and_pids)
        self.__send_message_to_queue(message)

        # Return pid
        logdebug(LOGGER, 'Making a PID for a data cart full of datasets... done.')
        loginfo(LOGGER, 'Requesting to create PID for data cart (%s).', cart_handle)
        return cart_handle

    '''
    Create the JSON message for creating the data cart PID.
    This calls the esgfpid.assistant.messages module.
    '''
    def __make_message(self, cart_handle, dict_of_drs_ids_and_pids):
        message_timestamp = esgfpid.utils.get_now_utc_as_formatted_string()
        message = esgfpid.assistant.messages.make_data_cart_message(
            cart_handle = cart_handle,
            timestamp = message_timestamp,
            data_cart_content = dict_of_drs_ids_and_pids
        )
        return message

    '''
    Send the JSON message to the RabbitMQ, using the
    esgfpid.coupling module.
    '''
    def __send_message_to_queue(self, message):
        self.__coupler.send_message_to_queue(message)
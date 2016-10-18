import logging
import esgfpid.utils
import esgfpid.assistant.messages

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class ShoppingCartAssistant(object):

    def __init__(self, **args):
        mandatory_args = ['prefix', 'coupler']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)
        self.__prefix = args['prefix']
        self.__coupler = args['coupler']

    def make_shopping_cart_pid(self, handles_or_strings):
        LOGGER.debug('Making a PID for a shopping cart full of datasets...')
        #mandatory_args = ['handles_or_strings']
        #esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        #esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)
        # TODO Title?

        # Make a pid (hash on the content):
        handles_or_strings = self.__get_handles_as_list(handles_or_strings)
        cart_handle = self.__get_handle_for_cart(handles_or_strings, self.__prefix)

        # Make and send message
        message = self.__make_message(cart_handle, handles_or_strings)
        self.__send_message_to_queue(message)

        # Return pid
        LOGGER.debug('Making a PID for a shopping cart full of datasets... done.')
        return cart_handle

    def __get_handles_as_list(self, handles):
        if type(handles) == type([]):
            pass
        else:
            handles = [handles]
        to_be_returned = esgfpid.utils.make_sorted_lowercase_list_without_hdl(handles)
        return to_be_returned

    def __get_handle_for_cart(self, handles_or_strings, prefix):
        return esgfpid.utils.make_handle_from_list_of_strings(handles_or_strings, prefix)
        # This sorts the list, removes all "hdl:", and makes a hash

    def __make_message(self, cart_handle, content_handles):
        message_timestamp = esgfpid.utils.get_now_utc_as_formatted_string()
        message = esgfpid.assistant.messages.make_shopping_cart_message(
            cart_handle = cart_handle,
            timestamp = message_timestamp,
            content_handles = content_handles
        )
        return message

    def __send_message_to_queue(self, message):
        self.__coupler.send_message_to_queue(message)
import logging
import esgfpid.rabbit
import esgfpid.solr
import esgfpid.utils

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
This class is responsible for communication with the
outside world, i.e. with solr and/or RabbitMQ.

Solr is needed for checking consistency, but these
checks are usually disabled.

The communication with RabbitMQ is crucial for the
library.

Everything that the library needs to know from solr
or RabbitMQ is done via this coupling module, which
basically just forwards all request to the responsible
module.
'''
class Coupler(object):

    '''
    Create a Coupler instance.

    :param messaging_service_credentials: Mandatory.
    :param messaging_service_exchange_name: Mandatory.
    :param message_service_synchronous: Mandatory. Boolean.
    :param test_publication: Mandatory. Boolean.

    :param solr_switched_off: Mandatory. Boolean.
    :param solr_url: Mandatory. May be None if switched off.
    :param handle_prefix: Mandatory. May be None if switched off.
    :param solr_https_verify: Mandatory. May be None if switched off.
    :param disable_insecure_request_warning: Mandatory. May be None if switched off.

    '''
    def __init__(self, **args):
        self.args = args # only for unit testing
        self.__create_message_sender(args)
        self.__create_solr_sender(args)     

    def __create_message_sender(self, args):
        self.__complete_credentials_for_open_nodes(args)
        self.__rabbit_message_sender = esgfpid.rabbit.RabbitMessageSender(
            exchange_name=args['messaging_service_exchange_name'],
            credentials=args['messaging_service_credentials'],
            test_publication=args['test_publication'],
            is_synchronous_mode=args['message_service_synchronous']
        )

    def __complete_credentials_for_open_nodes(self, args):
        credentials = args['messaging_service_credentials']
        for cred in credentials:
            if 'password' not in cred:
                cred['password'] = 'jzlnL78ZpExV#_QHz'

    def __create_solr_sender(self, args):
        self.__solr_sender = esgfpid.solr.SolrInteractor(
            solr_url=args['solr_url'],
            prefix=args['handle_prefix'],
            https_verify=args['solr_https_verify'],
            switched_off=args['solr_switched_off'],
            disable_insecure_request_warning = ['disable_insecure_request_warning']
        )

    ### Communications with rabbit

    '''
    Please see documentation of rabbit module (:func:`~rabbit.RabbitMessageSender.send_message_to_queue`).
    '''
    def send_message_to_queue(self, message):
        self.__rabbit_message_sender.send_message_to_queue(message)

    ### For synchronous

    '''
    Please see documentation of rabbit module (:func:`~rabbit.RabbitMessageSender.open_rabbit_connection`).
    '''
    def start_rabbit_business(self):
        self.__rabbit_message_sender.open_rabbit_connection()

    '''
    Please see documentation of rabbit module (:func:`~rabbit.RabbitMessageSender.close_rabbit_connection`).
    '''
    def done_with_rabbit_business(self):
        self.__rabbit_message_sender.close_rabbit_connection()

    ### For asynchronous

    '''
    Please see documentation of rabbit module (:func:`~rabbit.RabbitMessageSender.start`).
    '''
    def start_rabbit_connection(self):
        self.__rabbit_message_sender.start()

    '''
    Please see documentation of rabbit module (:func:`~rabbit.RabbitMessageSender.finish`).
    '''
    def finish_rabbit_connection(self):
        self.__rabbit_message_sender.finish()

    '''
    Please see documentation of rabbit module (:func:`~rabbit.RabbitMessageSender.force_finish`).
    '''
    def force_finish_rabbit_connection(self):
        self.__rabbit_message_sender.force_finish()

    ### Communications with solr

    '''
    Please see documentation of solr module (:func:`~solr.SolrInteractor.retrieve_datasethandles_or_versionnumbers_of_allversions`).
    '''
    def retrieve_datasethandles_or_versionnumbers_of_allversions(self, **args):
        mandatory_args = ['drs_id']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)

        result_dict = self.__solr_sender.retrieve_datasethandles_or_versionnumbers_of_allversions(
            drs_id=args['drs_id'],
        )
        return result_dict

    '''
    Please see documentation of solr module (:func:`~solr.SolrInteractor.retrieve_file_handles_of_same_dataset`).
    '''
    def retrieve_file_handles_of_same_dataset(self, **args):
        mandatory_args = ['drs_id', 'data_node', 'version_number']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)

        result_dict = self.__solr_sender.retrieve_file_handles_of_same_dataset(
            drs_id=args['drs_id'],
            data_node=args['data_node'],
            version_number=args['data_node']
        )
        return result_dict

    '''
    Please see documentation of solr module (:func:`~solr.SolrInteractor.is_switched_off`).
    '''
    def is_solr_switched_off(self):
        return self.__solr_sender.is_switched_off()
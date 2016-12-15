import logging
import esgfpid.rabbit.rabbit
import esgfpid.solr.solr
import esgfpid.utils

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class Coupler(object):

    def __init__(self, **args):
        self.__create_all_attributes()
        self.__create_message_sender(args)
        self.__create_solr_sender(args)

    def __create_all_attributes(self):
        self.__rabbit_message_sender = None
        self.__solr_sender = None

    def __create_message_sender(self, args):

        credentials = args['messaging_service_credentials']
        for cred in credentials:
            if 'password' not in cred:
                cred['password'] = 'jzlnL78ZpExV#_QHz'

        if 'test_publication' in args:
            test_publication = args['test_publication']
        else:
            test_publication = False

        self.__rabbit_message_sender = esgfpid.rabbit.rabbit.RabbitMessageSender(
            exchange_name=args['messaging_service_exchange_name'],       # mandatory
            credentials=args['messaging_service_credentials'],
            test_publication=test_publication
        )

    def __create_solr_sender(self, args):

        # Some use cases need no solr, so solr url is optional:
        if args['solr_url'] is None:
            args['solr_switched_off']=True

        self.__solr_sender = esgfpid.solr.solr.SolrInteractor(
            solr_url=args['solr_url'],
            prefix=args['handle_prefix'],
            https_verify=args['solr_https_verify'],
            switched_off=args['solr_switched_off']
        )

    ### Communications with rabbit

    def send_message_to_queue(self, message):
        success = self.__rabbit_message_sender.send_message_to_queue(message)
        return success

    ### For synchronous

    def start_rabbit_business(self):
        '''
        Open a connection. If not called, the first sent message automatically
        opens a connection.
        This is called for example by the publish assistant.
        '''
        self.__rabbit_message_sender.open_rabbit_connection()

    def done_with_rabbit_business(self):
        '''
        Optional.
        This is called for example by the publish assistant.
        '''
        self.__rabbit_message_sender.close_rabbit_connection()

    ### For asynchronous

    def start_rabbit_connection(self):
        self.__rabbit_message_sender.start()

    def finish_rabbit_connection(self):
        self.__rabbit_message_sender.finish()

    def force_finish_rabbit_connection(self):
        self.__rabbit_message_sender.force_finish()

    ### Communications with solr

    def retrieve_datasethandles_or_versionnumbers_of_allversions(self, **args):
        
        mandatory_args = ['drs_id']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)

        result_dict = self.__solr_sender.retrieve_datasethandles_or_versionnumbers_of_allversions(
            drs_id=args['drs_id'],
        )
        return result_dict

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

    def is_solr_switched_off(self):
        return self.__solr_sender.is_switched_off()
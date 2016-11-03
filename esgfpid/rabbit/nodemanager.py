import pika
import copy
import logging
import random
import esgfpid.defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class NodeManager(object):

    def __init__(self):

        # Props for basic_publish (needed by thread_feeder)
        self.__properties = pika.BasicProperties(
            delivery_mode=esgfpid.defaults.RABBIT_DELIVERY_MODE,
            content_type='application/json',
        )

        # Nodes
        self.__trusted_nodes = []
        self.__open_nodes = []
        self.__trusted_nodes_archive = copy.deepcopy(self.__trusted_nodes)
        self.__open_nodes_archive = copy.deepcopy(self.__open_nodes)

        # Current node
        self.__current_node = None

        # Important info
        self.__has_trusted = False

    def add_trusted_node(self, **kwargs):
        if self.__has_necessary_info(kwargs):
            node_info = copy.deepcopy(kwargs)
            self.__complete_info_dict(node_info, False)
            self.__trusted_nodes.append(node_info)
            self.__has_trusted = True
            logdebug(LOGGER, 'Trusted rabbit: %s, %s, %s', node_info['host'], node_info['username'], node_info['password'])

    def add_open_node(self, **kwargs):
        if self.__has_necessary_info(kwargs):
            node_info = copy.deepcopy(kwargs)
            self.__complete_info_dict(node_info, True)
            self.__open_nodes.append(node_info)
            logdebug(LOGGER, 'Open rabbit: %s, %s, %s', node_info['host'], node_info['username'], node_info['password'])

    def __has_necessary_info(self, node_info_dict):
        if ('username' in node_info_dict and
           'password' in node_info_dict and
           'host' in node_info_dict and 
           'exchange_name' in node_info_dict):
            return True
        else:
            return False
            # TODO Log what is missing

    def __complete_info_dict(self, node_info_dict, is_open):

        # Make pika credentials
        creds = pika.PlainCredentials(
            node_info_dict['username'],
            node_info_dict['password']
        )
        node_info_dict['credentials'] = creds

        # Get some defaults:
        socket_timeout = esgfpid.defaults.RABBIT_ASYN_SOCKET_TIMEOUT
        connection_attempts = esgfpid.defaults.RABBIT_ASYN_CONNECTION_ATTEMPTS
        retry_delay = esgfpid.defaults.RABBIT_ASYN_CONNECTION_RETRY_DELAY_SECONDS
        
        # Make pika connection params
        # https://pika.readthedocs.org/en/0.9.6/connecting.html
        params = pika.ConnectionParameters(
            host=node_info_dict['host'], # TODO: PORTS ETC.
            credentials=node_info_dict['credentials'],
            socket_timeout=socket_timeout,
            connection_attempts=connection_attempts,
            retry_delay=retry_delay
        )
        node_info_dict['params'] = params

        # Add some stuff
        node_info_dict['is_open'] = is_open
        '''
        https://pika.readthedocs.org/en/0.9.6/connecting.html
        class pika.connection.ConnectionParameters(
            host=None, port=None, virtual_host=None, credentials=None, channel_max=None,
            frame_max=None, heartbeat_interval=None, ssl=None, ssl_options=None,
            connection_attempts=None, retry_delay=None, socket_timeout=None, locale=None,
            backpressure_detection=None)
        '''
        return node_info_dict

    def get_connection_parameters(self):
        if self.__current_node is None:
            self.set_next_host()
        return self.__current_node['params']

    def has_more_urls(self):
        if self.get_num_left_urls() > 0:
            return True
        return False

    def get_num_left_urls(self):
        return len(self.__trusted_nodes) + len(self.__open_nodes)

    def set_next_host(self):

        if len(self.__trusted_nodes) == 1:
            self.__current_node = self.__trusted_nodes.pop()
            logdebug(LOGGER, 'Selecting the only trusted node: %s', self.__current_node['host'])

        elif len(self.__trusted_nodes) > 1:
            self.__current_node = self.__select_and_remove_random_url_from_list(self.__trusted_nodes)
            logdebug(LOGGER, 'Selecting a random trusted node: %s', self.__current_node['host'])

        elif len(self.__open_nodes) == 1:
            self.__current_node = self.__open_nodes.pop()
            logdebug(LOGGER, 'Selecting the only open node: %s', self.__current_node['host'])

        elif len(self.__open_nodes) > 1:
            self.__current_node = self.__select_and_remove_random_url_from_list(self.__open_nodes)
            logdebug(LOGGER, 'Selecting a random open node: %s', self.__current_node['host'])

        else:
            logwarn(LOGGER, 'No RabbitMQ node left to try! Leaving the last one: %s', self.__current_node['host'])

        self.__exchange_name = self.__current_node['exchange_name']

    ''' This returns always the same. Does not depend on node. '''
    def get_properties_for_message_publications(self):
        return self.__properties

    ''' This modifies the list! '''
    def __select_and_remove_random_url_from_list(self, list_urls):
        num_urls = len(list_urls)
        random_num = random.randint(0,num_urls-1)
        selected_url = list_urls[random_num]
        list_urls.remove(selected_url)
        return selected_url

    def get_exchange_name(self):
        return self.__exchange_name

    '''
    This flag is appended to the routing key 
    so that we can route messages from the untrusted nodes 
    to other queues.

    Note: The binding has to be done in the RabbitMQ exit 
    node (by the consumer).
    '''
    def get_open_word_for_routing_key(self):

        # Message is published via an open node:
        if self.__current_node['is_open'] == True:
            if self.__has_trusted:
                return 'untrusted-fallback'
            else:
                return 'untrusted-only'

        # Message is published via a trusted node:
        elif self.__current_node['is_open'] == False:
            return 'trusted'

        else:
            logerror(LOGGER, 'Problem: Unsure whether the current node is open or not!')
            return 'untrusted-unsure'

    def reset_nodes(self):
        self.__trusted_nodes = copy.deepcopy(self.__trusted_nodes_archive)
        self.__open_nodes = copy.deepcopy(self.__open_nodes_archive)
        self.set_next_host()
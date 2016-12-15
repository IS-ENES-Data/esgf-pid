import pika
import copy
import logging
import random
import esgfpid.defaults
import esgfpid.exceptions
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from naturalsorting import natural_keys

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
        self.__trusted_nodes = {}
        self.__open_nodes = {}
        self.__trusted_nodes_archive = {}
        self.__open_nodes_archive = {}

        # Current node
        self.__current_node = None

        # Important info
        self.__has_trusted = False

    def add_trusted_node(self, **kwargs):
        node_info = self.__add_node(self.__trusted_nodes, self.__trusted_nodes_archive, **kwargs)
        self.__has_trusted = True
        logdebug(LOGGER, 'Trusted rabbit: %s', self.__get_node_log_string(node_info))

    def add_open_node(self, **kwargs):
        added = node_info = self.__add_node(self.__open_nodes, self.__open_nodes_archive, **kwargs)
        logdebug(LOGGER, 'Open rabbit: %s', self.__get_node_log_string(node_info))

    def __add_node(self, store_where, store_archive, **kwargs):
        if self.__has_necessary_info(kwargs):
            node_info = copy.deepcopy(kwargs)
            self.__complete_info_dict(node_info, False)
            self.__store_node_info_by_priority(node_info, store_where)
            self.__store_node_info_by_priority(copy.deepcopy(node_info), store_archive)
            #store_where[node_info['priority']].append(node_info)
            #store_archive[node_info['priority']].append(copy.deepcopy(node_info))
            return node_info
        else:
            raise esgfpid.exceptions.ArgumentError('Cannot add this RabbitMQ node. Missing info. Required: username, password, host and exchange_name. Provided: '+str(kwargs))

    def __store_node_info_by_priority(self, node_info, store_where):
        try:
            store_where[node_info['priority']].append(node_info)
        except KeyError:
            store_where[node_info['priority']] = [node_info]

    def __get_node_log_string(self, node_info):
        return ('%s, %s, %s (exchange "%s")' % (node_info['host'], node_info['username'], node_info['password'], node_info['exchange_name']))

    def __has_necessary_info(self, node_info_dict):
        if ('username' in node_info_dict and
           'password' in node_info_dict and
           'host' in node_info_dict and 
           'exchange_name' in node_info_dict and
            node_info_dict['username'] is not None and
            node_info_dict['password'] is not None and
            node_info_dict['host'] is not None and
            node_info_dict['exchange_name'] is not None):
            return True
        else:
            return False

    def __complete_info_dict(self, node_info_dict, is_open):

        # Make pika credentials
        creds = pika.PlainCredentials(
            node_info_dict['username'],
            node_info_dict['password']
        )
        node_info_dict['credentials'] = creds
        if 'priority' in node_info_dict:
            node_info_dict['priority'] = str(node_info_dict['priority'])
        else:
            node_info_dict['priority'] = 'zzzz_last'

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

        if len(self.__trusted_nodes) > 0:
            self.__current_node = self.__get_highest_priority_node(self.__trusted_nodes)
            logdebug(LOGGER, 'Selected a trusted node: %s', self.__current_node['host'])

        elif len(self.__open_nodes) > 0:
            self.__current_node = self.__get_highest_priority_node(self.__open_nodes)
            logdebug(LOGGER, 'Selected an open node: %s', self.__current_node['host'])

        else:
            logwarn(LOGGER, 'No RabbitMQ node left to try! Leaving the last one: %s', self.__current_node['host'])

        self.__exchange_name = self.__current_node['exchange_name']

    def __get_highest_priority_node(self, dict_of_nodes):

        # Get highest priority:
        available_priorities = self.__trusted_nodes.keys()
        available_priorities.sort(key=natural_keys)
        current_priority = available_priorities.pop(0)
        list_of_priority_nodes = self.__trusted_nodes[current_priority]

        # Select one of them
        if len(list_of_priority_nodes)==1:
            nexthost = list_of_priority_nodes.pop()
            if len(list_of_priority_nodes)==0:
                self.__trusted_nodes.pop(current_priority)
            return nexthost
        else:
            nexthost = self.__select_and_remove_random_url_from_list(list_of_priority_nodes)
            return nexthost

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
        logdebug(LOGGER, 'Resetting hosts...')
        self.__trusted_nodes = copy.deepcopy(self.__trusted_nodes_archive)
        self.__open_nodes = copy.deepcopy(self.__open_nodes_archive)
        self.set_next_host()

    ''' Only for unit testing! '''
    def _get_num_trusted_and_open_nodes(self):
        n_open = len(self.__open_nodes_archive)
        n_trusted = len(self.__trusted_nodes_archive)
        return (n_trusted,n_open)

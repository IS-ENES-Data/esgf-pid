import pika
import copy
import logging
import random
import esgfpid.defaults
import esgfpid.exceptions
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times
from naturalsorting import natural_keys
import esgfpid.utils

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


DEFAULT_PRIO = 'zzz_default'
LAST_PRIO = 'zzzz_last'

'''
This class is responsible for keeping track of RabbitMQ
instances and providing tha access info to the library.

At startup, it is fed all the info about the various
instances, using "add_trusted_node()" and "add_open_node()".
During the library's functioning, it provides the info to
the library's RabbitMQ connection modules.

On every call of 

It is fed by all the RabbitMQ instances, their
access info and priority, in the beginning. Then it is
responsible for providing this info to the library.

It can deal with trusted and open nodes and with integer
priorities. It returns the instance access info dictionaries
in a well-defined order, 
'''
class NodeManager(object):

    '''
    Constructor that takes no params. It creates an empty
    container for RabbitMQ node information. The node 
    information then has to be added using "add_trusted_node()"
    and "add_open_node()".
    '''
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
        # Each of these dictionaries has the priorities as keys (integers
        # stored as strings, or 'zzz_default', if no prio was given, or
        # 'zzzz_last' if the host had failed in the pre-flight check).
        # For each priority, there is a list of node-info-dictionaries:
        # self.__trusted_nodes = {
        #     "1":         [node_info1, node_info2],
        #     "2":         [node_info3],
        #     "zzz_default": [node_info4]
        # }

        # Current node
        self.__current_node = None
        self.__exchange_name = None

        # Important info
        self.__has_trusted = False

    '''
    Add information about a trusted RabbitMQ node to
    the container, for later use.

    :param username: The username to connect to RabbitMQ.
    :param password: The password to connect to RabbitMQ.
    :param host: The host name of the RabbitMQ instance.
    :param exchange_name: The  exchange to which to send the
                          messages.
    :param priority: Optional. Integer priority for the use
                     of this instance.
    '''
    def add_trusted_node(self, **kwargs):
        kwargs['is_open'] = False
        node_info = self.__add_node(self.__trusted_nodes, self.__trusted_nodes_archive, **kwargs)
        self.__has_trusted = True
        logdebug(LOGGER, 'Trusted rabbit: %s', self.__get_node_log_string(node_info))

    '''
    Add information about an open RabbitMQ node to
    the container, for later use.

    The parameters that are needed are the same as
    for trusted nodes.

    Note that a password is needed!

    :param username: The username to connect to RabbitMQ.
    :param password: The password to connect to RabbitMQ.
    :param host: The host name of the RabbitMQ instance.
    :param exchange_name: The  exchange to which to send the
                          messages.
    :param priority: Optional. Integer priority for the use
                     of this instance.
    '''
    def add_open_node(self, **kwargs):
        raise esgfpid.exceptions.ArgumentError('Open nodes no longer supported! (Messaging service "'+kwargs['host']+'")')
        #kwargs['is_open'] = True
        #added = node_info = self.__add_node(self.__open_nodes, self.__open_nodes_archive, **kwargs)
        #logdebug(LOGGER, 'Open rabbit: %s', self.__get_node_log_string(node_info))

    def __add_node(self, store_where, store_archive, **kwargs):
        if self.__has_necessary_info(kwargs):
            node_info = copy.deepcopy(kwargs)
            self.__complete_info_dict(node_info, kwargs['is_open'])
            self.__store_node_info_by_priority(node_info, store_where)
            self.__store_node_info_by_priority(copy.deepcopy(node_info), store_archive)
            #store_where[node_info['priority']].append(node_info)
            #store_archive[node_info['priority']].append(copy.deepcopy(node_info))
            return node_info
        else:
            raise esgfpid.exceptions.ArgumentError('Cannot add this RabbitMQ node. Missing info. Required: username, password, host and exchange_name. Provided: '+str(kwargs))

    def __compare_nodes(self, cand1, cand2):
        copy1 = copy.deepcopy(cand1)
        copy2 = copy.deepcopy(cand2)
        # These cannot be compared by "==".
        # They are created from the other info, so neglecting
        # them in this comparison is ok!
        copy1['credentials'] = None
        copy2['credentials'] = None
        copy1['params'] = None
        copy2['params'] = None
        return copy1 == copy2

    def __is_this_node_in_last_prio_already(self, where_to_look):
        try:
            list_candidates = where_to_look[LAST_PRIO]
        except KeyError as e:
            errmsg = 'No node of last prio (%s) exists.' % LAST_PRIO
            logwarn(LOGGER, errmsg)
            return False

        for i in xrange(len(list_candidates)):
            candidate = list_candidates[i]
            if self.__compare_nodes(candidate,self.__current_node):
                logtrace(LOGGER, 'Found current node in archive (in list of last-prio nodes).')
                return True

        return False

    def __move_to_last_prio(self, current_prio, all_nodes):

        list_candidates = all_nodes[current_prio]
        loginfo(LOGGER, 'Nodes of prio "%s": %s', current_prio, list_candidates)

        for i in xrange(len(list_candidates)):
            candidate = list_candidates[i]
            if self.__compare_nodes(candidate,self.__current_node):
                logtrace(LOGGER, 'Found current node in archive.')

                # Add to lowest prio:
                try:
                    all_nodes[LAST_PRIO].append(candidate)
                    logdebug(LOGGER, 'Added this host to list of lowest prio hosts...')

                except KeyError:
                    all_nodes[LAST_PRIO] = [candidate]
                    logdebug(LOGGER, 'Added this host to (newly-created) list of lowest prio hosts...')

                # Remove from current prio:
                list_candidates.pop(i)
                loginfo(LOGGER, 'Removed this host from list of hosts with prio %s!', current_prio)
                if len(list_candidates)==0:
                    all_nodes.pop(current_prio)
                    loginfo(LOGGER, 'Removed the current priority %s!', current_prio)
                return True

        return False



    def set_priority_low_for_current(self):
        # We do not change the priority stored ass attribute in the
        # dicts, BUT we change the priority under which it is stored in
        # the list of nodes to be used.

        # Deal with open or trusted node:
        if self.__current_node['is_open']:
            where_to_look = self.__open_nodes_archive
        else:
            where_to_look = self.__trusted_nodes_archive

        # Go over all nodes of the current prio to find the
        # current one, then move it to a different prio:
        moved = False
        try:
            current_prio = self.__current_node['priority']
            moved = self.__move_to_last_prio(current_prio, where_to_look)
            if moved: return # changed successfully!

        except KeyError as e:
            errmsg = 'No node of prio %s found. Nodes: %s.' % (current_prio, where_to_look)
            logwarn(LOGGER, errmsg)

            # The node had already been added to the last-prio nodes ?!
            last_already = self.__is_this_node_in_last_prio_already(where_to_look)
            if last_already:
                logdebug(LOGGER, 'Node already had lowest priority.')
                return # nothing to change!

        # This is extremely unlikely - in fact I don't see how it could occur:
        if (not moved) and (not last_already):
            errmsg = 'Could not find this node\'s priority (%s), nor the last-priority (%s). Somehow this node\'s priority was changed weirdly.' % (current_prio, LAST_PRIO)
            logwarn(LOGGER, errmsg)
            logwarn(LOGGER, 'All nodes: %s' % where_to_look)

            # No matter where the node is stored, move it to "last" prio:
            for prio, nodes in where_to_look.iteritems():

                logtrace(LOGGER, 'Looking in prio "%s"...' % prio)
                moved = self.__move_to_last_prio(prio, where_to_look)
                if moved: return # changed successfully!

            errmsg = 'Node definitely not found, cannot change prio.'
            logwarn(LOGGER, errmsg)
            raise ValueError(errmsg)


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
        if 'priority' in node_info_dict and node_info_dict['priority'] is not None:
            node_info_dict['priority'] = str(node_info_dict['priority'])
        else:
            node_info_dict['priority'] = DEFAULT_PRIO

        # Mandatories:
        host = node_info_dict['host']
        credentials = node_info_dict['credentials']

        # Optional ones
        # If not specified, fill in defaults.
        vhost = ""
        if 'vhost' in node_info_dict and node_info_dict['vhost'] is not None:
            vhost = node_info_dict['vhost']
        port = 15672
        if 'port' in node_info_dict and node_info_dict['port'] is not None:
            port = node_info_dict['port']
        ssl_enabled = False
        if 'ssl_enabled' in node_info_dict and node_info_dict['ssl_enabled'] is not None:
            ssl_enabled = node_info_dict['ssl_enabled']


        # Get some defaults:
        socket_timeout = esgfpid.defaults.RABBIT_PIKA_SOCKET_TIMEOUT
        connection_attempts = esgfpid.defaults.RABBIT_PIKA_CONNECTION_ATTEMPTS
        retry_delay = esgfpid.defaults.RABBIT_PIKA_CONNECTION_RETRY_DELAY_SECONDS
        
        # Make pika connection params
        # https://pika.readthedocs.org/en/0.9.6/connecting.html
        params = pika.ConnectionParameters(
            host=host,
            ssl=ssl_enabled,
            port=port,
            virtual_host=vhost,
            credentials=credentials,
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

    '''
    Return the connection parameters for the current
    RabbitMQ host.

    :return: Connection parameters (of type pika.ConnectionParameters)
    '''
    def get_connection_parameters(self):
        if self.__current_node is None:
            self.set_next_host()
        if self.__current_node['is_open']:
            raise ArgumentError('Open nodes no longer supported! (Messaging service "'+credentials['url']+'")')
        return self.__current_node['params']

    '''
    Simple getter to find out if any URLs are
    left.

    TODO: Needed for what, as we start over
    once all have been used? There is no end!

    :return: Boolean.
    '''

    def has_more_urls(self):
        if self.get_num_left_urls() > 0:
            return True
        return False

    '''
    Compute and return the number of trusted
    RabbitMQ instances.

    :return: Number of trusted instances (integer).
    '''
    def get_num_left_trusted(self):
        n_trusted = 0
        for list_of_nodes in self.__trusted_nodes.values():
            n_trusted = n_trusted + len(list_of_nodes)
        return n_trusted

    '''
    Compute and return the number of open
    RabbitMQ instances.

    :return: Number of open instances (integer).
    '''
    def get_num_left_open(self):
        n_open = 0
        for list_of_nodes in self.__open_nodes.values():
            n_open = n_open + len(list_of_nodes)
        return n_open

    '''
    Compute and return the total number of RabbitMQ
    instances.

    :return: Number of trusted instances (integer).
    '''
    def get_num_left_urls(self):
        return self.get_num_left_open() + self.get_num_left_trusted()


    '''
    Select the next RabbitMQ to be used, using the
    predefined priorities. It is not returned.
    '''
    def set_next_host(self):

        if len(self.__trusted_nodes) > 0:
            self.__current_node = self.__get_highest_priority_node(self.__trusted_nodes)
            logdebug(LOGGER, 'Selected a trusted node: %s', self.__current_node['host'])

        elif len(self.__open_nodes) > 0:
            self.__current_node = self.__get_highest_priority_node(self.__open_nodes)
            logdebug(LOGGER, 'Selected an open node: %s', self.__current_node['host'])

        else:
            if self.__current_node is None:
                logwarn(LOGGER, 'Unexpected: No RabbitMQ node left to try, and there is no current one.')
                raise esgfpid.exceptions.ArgumentError('No RabbitMQ nodes were passed at all.')
            logwarn(LOGGER, 'No RabbitMQ node left to try! Leaving the last one: %s', self.__current_node['host'])

        self.__exchange_name = self.__current_node['exchange_name']

    def __get_highest_priority_node(self, dict_of_nodes):

        # Get highest priority:
        available_priorities = dict_of_nodes.keys()
        available_priorities.sort(key=natural_keys)
        current_priority = available_priorities.pop(0)
        list_of_priority_nodes = dict_of_nodes[current_priority]

        # Select one of them
        if len(list_of_priority_nodes)==1:
            nexthost = list_of_priority_nodes.pop()
            if len(list_of_priority_nodes)==0:
                dict_of_nodes.pop(current_priority)
            return nexthost
        else:
            nexthost = self.__select_and_remove_random_url_from_list(list_of_priority_nodes)
            return nexthost
        # TODO WHAT IF NONE LEFT???

    '''
    Return a pika.BasicProperties object needed for
    connecting to RabbitMQ.

    This returns always the same. Does not depend on node.

    :return: A  properties object (pika.BasicProperties).'''
    def get_properties_for_message_publications(self):
        return self.__properties

    '''
    Select and return a random URL from a list.
    This modifies the list and returns the URL!

    :param list_urls: The list of URLs to randomly select from.
    :return: Randomly selected URL.
    '''
    def __select_and_remove_random_url_from_list(self, list_urls):
        num_urls = len(list_urls)
        random_num = random.randint(0,num_urls-1)
        selected_url = list_urls[random_num]
        list_urls.remove(selected_url)
        return selected_url

    '''
    Return the current exchange name as string.

    :return: The current exchange name.
    '''
    def get_exchange_name(self):
        return self.__exchange_name

    '''
    Adapt the routing key (if necessary) to indicate 
    that a message comes from an untrusted node.

    The middle part (the RabbitMQ instruction part)
    of the key is adapted so it is visible that the
    node is an untrusted one.

    Note: The binding has to be done in the RabbitMQ exit 
    node (by the consumer).

    :return: The adapted routing key, in case the node
             is untrusted.
    '''
    def adapt_routing_key_for_untrusted(self, routing_key):

        # Message is published via an open node:
        if self.__current_node['is_open'] == True:
            if self.__has_trusted:
                return esgfpid.utils.adapt_routing_key_for_untrusted_fallback(routing_key)
            else:
                return esgfpid.utils.adapt_routing_key_for_untrusted(routing_key)

        # Message is published via a trusted node:
        elif self.__current_node['is_open'] == False:
            return routing_key
        else:
            logerror(LOGGER, 'Problem: Unsure whether the current node is open or not!')
            return esgfpid.utils.adapt_routing_key_for_untrusted_fallback(routing_key)

    '''
    Reset the list of available RabbitMQ instances to
    how it was before trying any.
    Once all RabbitMQ instances have been tried,
    the list is reset, so we can start over trying
    to connect.

    TODO: Where is this called?
    '''
    def reset_nodes(self):
        logdebug(LOGGER, 'Resetting hosts...')
        self.__trusted_nodes = copy.deepcopy(self.__trusted_nodes_archive)
        self.__open_nodes = copy.deepcopy(self.__open_nodes_archive)
        self.set_next_host()

    def _get_prio_stored_for_current(self):
        # Currently only used in unit test
        return self.__current_node['priority']

    def _get_prio_where_current_is_stored(self):
        # Currently only used in unit test

        if self.__current_node['is_open']:
            where_to_look = self.__open_nodes_archive
        else:
            where_to_look = self.__trusted_nodes_archive

        if not type(where_to_look) == type(dict()):
            raise ValueError('%s is not a dict!')

        for prio, nodes in where_to_look.iteritems():
            for candidate in nodes:
                if self.__compare_nodes(self.__current_node, candidate):
                    return prio

        raise ValueError('Node not found, so could not know currently active prio!')

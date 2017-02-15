import unittest
import mock
import logging
import json
import sys
import os
import pika
sys.path.append("..")
import esgfpid.rabbit

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class RabbitNodemanagerTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ########## (%s)' % os.path.basename(__file__))

    def tearDown(self):
        LOGGER.info('#############################')

    def __get_args_dict(self, **kwargs):
        args = dict(
            exchange_name='exch_foo',
            host='host_foo',
            username='user_foo',
            password='pw_foo'
        )
        if 'exchange_name' in kwargs:
            args['exchange_name'] = kwargs['exchange_name']
        if 'username' in kwargs:
            args['username'] = kwargs['username']
        if 'password' in kwargs:
            args['password'] = kwargs['password']
        if 'priority' in kwargs:
            args['priority'] = kwargs['priority']
        if 'host' in kwargs:
            args['host'] = kwargs['host']
        return args


    # Tests

    #
    # Test the constructor.
    #

    '''
    Simple test whether the constructor works.
    It takes no args at all.
    '''
    def test_constructor(self):

        # Run code to be tested:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()

        # Check result
        self.assertIsInstance(mynodemanager, esgfpid.rabbit.nodemanager.NodeManager)

    '''
    Test if I can pass a trusted node with no priority.
    '''
    def test_add_one_trusted_node_no_prio_ok(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()

        # Run code to be tested:
        mynodemanager.add_trusted_node(**self.__get_args_dict())

        # Check whether the correct number of nodes is there:
        self.assertEquals(mynodemanager.get_num_left_open(),0)
        self.assertEquals(mynodemanager.get_num_left_trusted(),1)
        self.assertEquals(mynodemanager.get_num_left_urls(),1)

        # Check whether the correct priorities are set:
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEquals(node['priority'], 'zzzz_last')
        self.assertFalse(node['is_open'])
        self.assertIsInstance(node['credentials'], pika.PlainCredentials)
        self.assertIsInstance(node['params'], pika.ConnectionParameters)

    '''
    Test if I can pass a trusted node with priority=None.
    '''
    def test_add_one_trusted_node_prio_none_ok(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        args = self.__get_args_dict(priority=None)

        # Run code to be tested:
        mynodemanager.add_trusted_node(**args)

        # Check whether the correct number of nodes is there:
        self.assertEquals(mynodemanager.get_num_left_open(),0)
        self.assertEquals(mynodemanager.get_num_left_trusted(),1)
        self.assertEquals(mynodemanager.get_num_left_urls(),1)

        # Check whether the correct priorities are set:
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEquals(node['priority'], 'zzzz_last')
        self.assertFalse(node['is_open'])
        self.assertIsInstance(node['credentials'], pika.PlainCredentials)
        self.assertIsInstance(node['params'], pika.ConnectionParameters)

    '''
    Test if I can pass several trusted nodes with no priority.
    '''
    def test_add_trusted_nodes_no_prio_ok(self):

        # Test variables:
        # Make test node manager:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()

        # Run code to be tested:
        mynodemanager.add_trusted_node(**self.__get_args_dict())
        mynodemanager.add_trusted_node(**self.__get_args_dict())

        # Check whether the correct number of nodes is there:
        self.assertEquals(mynodemanager.get_num_left_open(),0)
        self.assertEquals(mynodemanager.get_num_left_trusted(),2)
        self.assertEquals(mynodemanager.get_num_left_urls(),2)

    '''
    Test if I can pass several trusted nodes with priority.
    '''
    def test_add_trusted_nodes_with_prio_ok(self):

        # Test variables:
        # Make test node manager, and node-dictionaries with
        # various priorities:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        args1 = self.__get_args_dict(priority=3)
        args2 = self.__get_args_dict(priority=1)
        # Run code to be tested:
        mynodemanager.add_trusted_node(**args1)
        mynodemanager.add_trusted_node(**args2)

        # Check whether the correct number of nodes is there:
        self.assertEquals(mynodemanager.get_num_left_open(),0)
        self.assertEquals(mynodemanager.get_num_left_trusted(),2)
        self.assertEquals(mynodemanager.get_num_left_urls(),2)
        # Check whether the correct priorities are set:
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEquals(node['priority'], '1')
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEquals(node['priority'], '3')

    '''
    Test exception if I miss info
    '''
    def test_add_nodes_missing_info(self):

        # Test variables:
        # Make test node manager:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            mynodemanager.add_trusted_node(username='foo', exchange_name='bar')
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            mynodemanager.add_open_node(username='foo', exchange_name='bar')



    '''
    Test the algorithm that picks the next host, based on
    passed priorities.
    '''
    def test_priority_algorithm(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        args1 = self.__get_args_dict(host='foo1', priority=1)
        args2 = self.__get_args_dict(host='foo2', priority=3)
        args3 = self.__get_args_dict(host='foo3', priority=3)
        args4 = self.__get_args_dict(host='foo4')
        args1o = self.__get_args_dict(host='foo1o', priority=1)
        args2o = self.__get_args_dict(host='foo2o', priority=3)
        args3o = self.__get_args_dict(host='foo3o', priority=3)
        args4o = self.__get_args_dict(host='foo4o')
        # Add the nodes, in no particular order:
        mynodemanager.add_open_node(**args2o)
        mynodemanager.add_open_node(**args4o)
        mynodemanager.add_open_node(**args1o)
        mynodemanager.add_open_node(**args3o)
        mynodemanager.add_trusted_node(**args2)
        mynodemanager.add_trusted_node(**args4)
        mynodemanager.add_trusted_node(**args3)
        mynodemanager.add_trusted_node(**args1)

        # Run code to be tested:
        # Set one host after the other and check whether the
        # correct host was picked, based on the passed priority.
        # The first one should be foo1
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEquals(node['host'], 'foo1')
        self.assertFalse(node['is_open'])
        # The second one should be foo2 OR foo3 (same prio)
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertTrue('foo2' in node['host'] or 'foo3' in node['host'])
        self.assertFalse(node['is_open'])
        # The third one should be foo2 OR foo3 (same prio)
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertTrue('foo2' in node['host'] or 'foo3' in node['host'])
        self.assertFalse(node['is_open'])
        # The fourth one should be foo4
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEquals(node['host'], 'foo4')
        self.assertFalse(node['is_open'])
        # The fifth one should be foo1o (the first open)
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEquals(node['host'], 'foo1o')
        self.assertTrue(node['is_open'])
        # The sixth one should be foo2o OR foo3o (same prio)
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertTrue('foo2o' in node['host'] or 'foo3o' in node['host'])
        self.assertTrue(node['is_open'])
        # The seventh one should be foo2 OR foo3 (same prio)
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertTrue('foo2o' in node['host'] or 'foo3o' in node['host'])
        self.assertTrue(node['is_open'])
        # The eigth one should be foo4o
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEquals(node['host'], 'foo4o')
        self.assertTrue(node['is_open'])

    '''
    Test getter for exchange name.
    '''
    def test_get_exchange_name(self):

        # Make node manager:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        # Add the nodes, in no particular order:
        mynodemanager.add_trusted_node(**self.__get_args_dict(exchange_name='foo2', priority=6))
        mynodemanager.add_open_node(**self.__get_args_dict(exchange_name='foo3', priority=1))
        mynodemanager.add_trusted_node(**self.__get_args_dict(exchange_name='foo1', priority=2))

        # Run code to be tested:
        # Set one host after the other and check if the correct
        # exchange name is returned.
        mynodemanager.set_next_host()
        self.assertEquals('foo1', mynodemanager.get_exchange_name())
        mynodemanager.set_next_host()
        self.assertEquals('foo2', mynodemanager.get_exchange_name())
        mynodemanager.set_next_host()
        self.assertEquals('foo3', mynodemanager.get_exchange_name())


    '''
    Test behaviour of set_next_host() if none is left.
    '''
    def test_setting_nodes_if_none_left(self):

        # Make a node manager with several nodes, and remove them
        # one by one.
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**self.__get_args_dict(exchange_name='foo', priority=1))
        mynodemanager.add_trusted_node(**self.__get_args_dict(exchange_name='bar', priority=2))

        # Run code to be tested and check results:
        # Set the first host, with exchange=foo
        mynodemanager.set_next_host()
        self.assertEquals(mynodemanager.get_exchange_name(), 'foo')
        # Set the second host, with exchange=bar
        mynodemanager.set_next_host()
        self.assertEquals(mynodemanager.get_exchange_name(), 'bar')
        # If we continue trying to set a next one, it stays "bar",
        # as no other is left!
        mynodemanager.set_next_host()
        self.assertEquals(mynodemanager.get_exchange_name(), 'bar')
        mynodemanager.set_next_host()
        self.assertEquals(mynodemanager.get_exchange_name(), 'bar')
        mynodemanager.set_next_host()
        self.assertEquals(mynodemanager.get_exchange_name(), 'bar')

    '''
    Test whether setting and resetting nodes works.
    '''
    def test_setting_and_resetting_nodes(self):

        # Make a node manager with several nodes, and remove them
        # one by one.
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**self.__get_args_dict())
        mynodemanager.add_trusted_node(**self.__get_args_dict())
        mynodemanager.add_trusted_node(**self.__get_args_dict())
        mynodemanager.add_open_node(**self.__get_args_dict())
        mynodemanager.add_open_node(**self.__get_args_dict())

        # Run code to be tested:
        # First, set one after the other, and check if the
        # number of URLs left is correct.
        # When all are used, reset the hosts and check if
        # the resetting worked fine.

        # We added five hosts, 3 trusted and 2 untrusted:
        self.assertEquals(mynodemanager.get_num_left_urls(),5)
        self.assertEquals(mynodemanager.get_num_left_trusted(),3)
        self.assertEquals(mynodemanager.get_num_left_open(),2)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the first host (1/5), 4 are left. (First, the trusted ones are selected):
        mynodemanager.set_next_host()
        self.assertEquals(mynodemanager.get_num_left_urls(),4)
        self.assertEquals(mynodemanager.get_num_left_trusted(),2)
        self.assertEquals(mynodemanager.get_num_left_open(),2)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the second host (2/5), 3 are left.
        mynodemanager.set_next_host()
        self.assertEquals(mynodemanager.get_num_left_urls(),3)
        self.assertEquals(mynodemanager.get_num_left_trusted(),1)
        self.assertEquals(mynodemanager.get_num_left_open(),2)
        # Set the third host (3/5), 2 are left (the two untrusted ones):
        mynodemanager.set_next_host()
        self.assertEquals(mynodemanager.get_num_left_urls(),2)
        self.assertEquals(mynodemanager.get_num_left_trusted(),0)
        self.assertEquals(mynodemanager.get_num_left_open(),2)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the fourth host (4/5), 1 is left:
        mynodemanager.set_next_host()
        self.assertEquals(mynodemanager.get_num_left_urls(),1)
        self.assertEquals(mynodemanager.get_num_left_trusted(),0)
        self.assertEquals(mynodemanager.get_num_left_open(),1)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the last host (5/5), so 0 are left:
        mynodemanager.set_next_host()
        self.assertEquals(mynodemanager.get_num_left_urls(),0)
        self.assertEquals(mynodemanager.get_num_left_trusted(),0)
        self.assertEquals(mynodemanager.get_num_left_open(),0)
        self.assertFalse(mynodemanager.has_more_urls())
        # Reset all nodes.
        # This includes the first call to set_next_host(), so it 
        # sets the first of five, so 4 are left:
        mynodemanager.reset_nodes()
        self.assertEquals(mynodemanager.get_num_left_urls(),4)
        self.assertEquals(mynodemanager.get_num_left_trusted(),2)
        self.assertEquals(mynodemanager.get_num_left_open(),2)
        self.assertTrue(mynodemanager.has_more_urls())

    '''
    Test getter for the routing key suffix (1/3).
    '''
    def test_open_word_open_only(self):

        # Test variables:
        # Make a node manager with only open nodes
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_open_node(**self.__get_args_dict())
        mynodemanager.add_open_node(**self.__get_args_dict())
        # Pre-check:
        self.assertEquals(mynodemanager.get_num_left_trusted(),0, 'Failing precheck.')
        self.assertEquals(mynodemanager.get_num_left_open(),2, 'Failing precheck.')
        # A node has to be set currently for this to work:
        mynodemanager.set_next_host()
        #print(str(mynodemanager._NodeManager__current_node))

        # Run code to be tested:
        word = mynodemanager.get_open_word_for_routing_key()

        # Check result:
        # Check if get_open_word_for_routing_key() returns
        # the correct routing key suffix. If there is only open
        # nodes, it must indicate that it's open nodes only.
        self.assertEquals('untrusted-only', word)

    '''
    Test getter for the routing key suffix (2/3).
    '''
    def test_open_word_trusted_only(self):

        # Test variables:
        # Make a node manager with only open nodes
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**self.__get_args_dict())
        mynodemanager.add_trusted_node(**self.__get_args_dict())
        # Pre-check:
        self.assertEquals(mynodemanager.get_num_left_trusted(),2, 'Failing precheck.')
        self.assertEquals(mynodemanager.get_num_left_open(),0, 'Failing precheck.')
        # A node has to be set currently for this to work:
        mynodemanager.set_next_host()
        #print(str(mynodemanager._NodeManager__current_node))

        # Run code to be tested
        word = mynodemanager.get_open_word_for_routing_key()

        # Check result:
        # Check if get_open_word_for_routing_key() returns
        # the correct routing key suffix.
        # If there is trusted nodes, and a trusted node is
        # currently set, it must indicate that it's trusted.
        self.assertEquals('trusted', word)

    '''
    Test getter for the routing key suffix (3/3).
    '''
    def test_open_word_open_fallback(self):

        # Make a node manager with only open nodes
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_open_node(**self.__get_args_dict())
        mynodemanager.add_trusted_node(**self.__get_args_dict())
        # Pre-check:
        self.assertEquals(mynodemanager.get_num_left_trusted(),1, 'Failing precheck.')
        self.assertEquals(mynodemanager.get_num_left_open(),1, 'Failing precheck.')
        # A node has to be set currently for this to work.
        # We want to test the situation where the OPEN one is
        # set. The first call to set_next_host() sets the trusted
        # one:
        mynodemanager.set_next_host()
        #print(str(mynodemanager._NodeManager__current_node))
        # The second call sets the open one, which is what we want:
        mynodemanager.set_next_host()
        #print(str(mynodemanager._NodeManager__current_node))

        # Run code to be tested
        word = mynodemanager.get_open_word_for_routing_key()

        # Check result:
        # Check if get_open_word_for_routing_key() returns
        # the correct routing key suffix.
        # If there is both types of nodes, but an open is
        # currently set, it must indicate that it's open, but
        # only for fallback reasons, as the trusted ones must
        # have failed for the open to be set.
        self.assertEquals('untrusted-fallback', word)

    '''
    Test getter for parameters for current host.
    '''
    def test_get_params(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**self.__get_args_dict(host='foo'))
        mynodemanager.add_open_node(**self.__get_args_dict(host='bar'))

        # Run code to be tested (1/2):
        #mynodemanager.set_next_host() # This is called by the getter!!
        params = mynodemanager.get_connection_parameters()

        # Check result (1/2):
        self.assertIsInstance(params, pika.ConnectionParameters)
        self.assertEquals(params.host, 'foo')

        # Run code to be tested (2/2):
        mynodemanager.set_next_host()
        params = mynodemanager.get_connection_parameters()

        # Check result (2/2):
        self.assertIsInstance(params, pika.ConnectionParameters)
        self.assertEquals(params.host, 'bar')


    '''
    Test simple getter.
    '''
    def test_get_props(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()

        # Run code to be tested:
        props = mynodemanager.get_properties_for_message_publications()

        # Check result
        self.assertIsInstance(props, pika.BasicProperties)
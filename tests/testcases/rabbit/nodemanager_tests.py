

import unittest
import logging
import pika
import esgfpid.rabbit
import tests.globalvar

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
import resources.TESTVALUES as TESTHELPERS

# Some tests rely on open nodes
import globalvar
if globalvar.RABBIT_OPEN_NOT_ALLOWED:
    print('Skipping tests that need open RabbitMQ nodes in module "%s".' % __name__)


'''
Unit tests for esgfpid.rabbit.nodemanager.

This module does not need any other module to
function, so we don't need to use or to mock any
other objects. 
'''
class NodemanagerTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

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
        args = TESTHELPERS.get_args_for_nodemanager()
        mynodemanager.add_trusted_node(**args)

        # Check whether the correct number of nodes is there:
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        self.assertEqual(mynodemanager.get_num_left_trusted(),1)
        self.assertEqual(mynodemanager.get_num_left_urls(),1)

        # Check whether the correct priorities are set:
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEqual(node['priority'], 'zzz_default')
        self.assertFalse(node['is_open'])
        self.assertIsInstance(node['credentials'], pika.PlainCredentials)
        self.assertIsInstance(node['params'], pika.ConnectionParameters)

    '''
    Test if I can pass a trusted node with priority=None.
    '''
    def test_add_one_trusted_node_prio_none_ok(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        args = TESTHELPERS.get_args_for_nodemanager(priority=None, vhost='foo', port=22, ssl_enabled=True)

        # Run code to be tested:
        mynodemanager.add_trusted_node(**args)

        # Check whether the correct number of nodes is there:
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        self.assertEqual(mynodemanager.get_num_left_trusted(),1)
        self.assertEqual(mynodemanager.get_num_left_urls(),1)

        # Check whether the correct priorities are set:
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEqual(node['priority'], 'zzz_default')
        self.assertFalse(node['is_open'])
        self.assertIsInstance(node['credentials'], pika.PlainCredentials)
        self.assertIsInstance(node['params'], pika.ConnectionParameters)
        self.assertEqual(node['vhost'], 'foo')
        self.assertEqual(node['port'], 22)
        self.assertEqual(node['ssl_enabled'], True)

    '''
    Test if I can pass several trusted nodes with no priority.
    '''
    def test_add_trusted_nodes_no_prio_ok(self):

        # Test variables:
        # Make test node manager:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        args = TESTHELPERS.get_args_for_nodemanager()

        # Run code to be tested:
        mynodemanager.add_trusted_node(**args)
        mynodemanager.add_trusted_node(**args)

        # Check whether the correct number of nodes is there:
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        self.assertEqual(mynodemanager.get_num_left_trusted(),2)
        self.assertEqual(mynodemanager.get_num_left_urls(),2)

    '''
    Test if I can pass several trusted nodes with priority.
    '''
    def test_add_trusted_nodes_with_prio_ok(self):

        # Test variables:
        # Make test node manager, and node-dictionaries with
        # various priorities:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        args1 = TESTHELPERS.get_args_for_nodemanager(priority=3)
        args2 = TESTHELPERS.get_args_for_nodemanager(priority=1)
        # Run code to be tested:
        mynodemanager.add_trusted_node(**args1)
        mynodemanager.add_trusted_node(**args2)

        # Check whether the correct number of nodes is there:
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        self.assertEqual(mynodemanager.get_num_left_trusted(),2)
        self.assertEqual(mynodemanager.get_num_left_urls(),2)
        # Check whether the correct priorities are set:
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEqual(node['priority'], '1')
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEqual(node['priority'], '3')

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

    '''
    Test exception if I miss info, and check if password is printed
    (it should not!)
    '''
    def test_add_nodes_missing_info_2(self):

        # Test variables:
        # Make test node manager:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()

        # Run code to be tested and check exception:
        with self.assertRaises(esgfpid.exceptions.ArgumentError) as context_manager:
            mynodemanager.add_trusted_node(username='foo', exchange_name='bar',
                password='einekleinenachtmusik')

        # Check exception content:
        ex = context_manager.exception
        self.assertTrue('einekleinenachtmusik' not in ex.msg, ex)

    '''
    Test exception if I miss info
    '''
    @unittest.skipIf(globalvar.RABBIT_OPEN_NOT_ALLOWED, '(this test uses open rabbit nodes)')
    def test_add_nodes_missing_info_open(self):

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
    def test_priority_algorithm_trusted_only(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        args1 = TESTHELPERS.get_args_for_nodemanager(host='foo1', priority=1)
        args2 = TESTHELPERS.get_args_for_nodemanager(host='foo2', priority=3)
        args3 = TESTHELPERS.get_args_for_nodemanager(host='foo3', priority=3)
        args4 = TESTHELPERS.get_args_for_nodemanager(host='foo4')
        # Add the nodes, in no particular order:
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
        self.assertEqual(node['host'], 'foo1')
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
        self.assertEqual(node['host'], 'foo4')
        self.assertFalse(node['is_open'])


    '''
    Test the algorithm that picks the next host, based on
    passed priorities.
    '''
    @unittest.skipIf(globalvar.RABBIT_OPEN_NOT_ALLOWED, '(this test uses open rabbit nodes)')
    def test_priority_algorithm_open(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        args1 = TESTHELPERS.get_args_for_nodemanager(host='foo1', priority=1)
        args2 = TESTHELPERS.get_args_for_nodemanager(host='foo2', priority=3)
        args3 = TESTHELPERS.get_args_for_nodemanager(host='foo3', priority=3)
        args4 = TESTHELPERS.get_args_for_nodemanager(host='foo4')
        args1o = TESTHELPERS.get_args_for_nodemanager(host='foo1o', priority=1)
        args2o = TESTHELPERS.get_args_for_nodemanager(host='foo2o', priority=3)
        args3o = TESTHELPERS.get_args_for_nodemanager(host='foo3o', priority=3)
        args4o = TESTHELPERS.get_args_for_nodemanager(host='foo4o')
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
        self.assertEqual(node['host'], 'foo1')
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
        self.assertEqual(node['host'], 'foo4')
        self.assertFalse(node['is_open'])
        # The fifth one should be foo1o (the first open)
        mynodemanager.set_next_host()
        node = mynodemanager._NodeManager__current_node
        self.assertEqual(node['host'], 'foo1o')
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
        self.assertEqual(node['host'], 'foo4o')
        self.assertTrue(node['is_open'])

    '''
    Test getter for exchange name.
    '''
    def test_get_exchange_name(self):

        # Make node manager:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        # Add the nodes, in no particular order:
        args1 = TESTHELPERS.get_args_for_nodemanager(exchange_name='foo2', priority=6)
        args2 = TESTHELPERS.get_args_for_nodemanager(exchange_name='foo3', priority=1)
        args3 = TESTHELPERS.get_args_for_nodemanager(exchange_name='foo1', priority=2)
        mynodemanager.add_trusted_node(**args1)
        mynodemanager.add_trusted_node(**args2)
        mynodemanager.add_trusted_node(**args3)

        # Run code to be tested:
        # Set one host after the other and check if the correct
        # exchange name is returned.
        mynodemanager.set_next_host()
        self.assertEqual('foo3', mynodemanager.get_exchange_name())
        mynodemanager.set_next_host()
        self.assertEqual('foo1', mynodemanager.get_exchange_name())
        mynodemanager.set_next_host()
        self.assertEqual('foo2', mynodemanager.get_exchange_name())

    '''
    Test getter for exchange name.
    '''
    @unittest.skipIf(globalvar.RABBIT_OPEN_NOT_ALLOWED, '(this test uses open rabbit nodes)')
    def test_get_exchange_name_open(self):

        # Make node manager:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        # Add the nodes, in no particular order:
        args1 = TESTHELPERS.get_args_for_nodemanager(exchange_name='foo2', priority=6)
        args2 = TESTHELPERS.get_args_for_nodemanager(exchange_name='foo3', priority=1)
        args3 = TESTHELPERS.get_args_for_nodemanager(exchange_name='foo1', priority=2)
        mynodemanager.add_trusted_node(**args1)
        mynodemanager.add_open_node(**args2)
        mynodemanager.add_trusted_node(**args3)

        # Run code to be tested:
        # Set one host after the other and check if the correct
        # exchange name is returned.
        mynodemanager.set_next_host()
        self.assertEqual('foo1', mynodemanager.get_exchange_name())
        mynodemanager.set_next_host()
        self.assertEqual('foo2', mynodemanager.get_exchange_name())
        mynodemanager.set_next_host()
        self.assertEqual('foo3', mynodemanager.get_exchange_name())

    '''
    Test behaviour of set_next_host() if none is left.
    '''
    def test_setting_nodes_if_none_left(self):

        # Make a node manager with several nodes, and remove them
        # one by one.
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        args1 = TESTHELPERS.get_args_for_nodemanager(exchange_name='foo', priority=1)
        args2 = TESTHELPERS.get_args_for_nodemanager(exchange_name='bar', priority=2)
        mynodemanager.add_trusted_node(**args1)
        mynodemanager.add_trusted_node(**args2)

        # Run code to be tested and check results:
        # Set the first host, with exchange=foo
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_exchange_name(), 'foo')
        # Set the second host, with exchange=bar
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_exchange_name(), 'bar')
        # If we continue trying to set a next one, it stays "bar",
        # as no other is left!
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_exchange_name(), 'bar')
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_exchange_name(), 'bar')
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_exchange_name(), 'bar')

    '''
    Test whether setting and resetting nodes works.
    '''
    def test_setting_and_resetting_nodes_trusted_only(self):

        # Make a node manager with several nodes, and remove them
        # one by one.
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())

        # Run code to be tested:
        # First, set one after the other, and check if the
        # number of URLs left is correct.
        # When all are used, reset the hosts and check if
        # the resetting worked fine.

        # We added five hosts, 3 trusted and 2 untrusted:
        self.assertEqual(mynodemanager.get_num_left_urls(),5)
        self.assertEqual(mynodemanager.get_num_left_trusted(),5)
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the first host (1/5), 4 are left. (First, the trusted ones are selected):
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_num_left_urls(),4)
        self.assertEqual(mynodemanager.get_num_left_trusted(),4)
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the second host (2/5), 3 are left.
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_num_left_urls(),3)
        self.assertEqual(mynodemanager.get_num_left_trusted(),3)
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        # Set the third host (3/5), 2 are left (the two untrusted ones):
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_num_left_urls(),2)
        self.assertEqual(mynodemanager.get_num_left_trusted(),2)
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the fourth host (4/5), 1 is left:
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_num_left_urls(),1)
        self.assertEqual(mynodemanager.get_num_left_trusted(),1)
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the last host (5/5), so 0 are left:
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_num_left_urls(),0)
        self.assertEqual(mynodemanager.get_num_left_trusted(),0)
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        self.assertFalse(mynodemanager.has_more_urls())
        # Reset all nodes.
        # This includes the first call to set_next_host(), so it 
        # sets the first of five, so 4 are left:
        mynodemanager.reset_nodes()
        self.assertEqual(mynodemanager.get_num_left_urls(),4)
        self.assertEqual(mynodemanager.get_num_left_trusted(),4)
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        self.assertTrue(mynodemanager.has_more_urls())

    '''
    Test whether setting and resetting nodes works.
    '''
    @unittest.skipIf(globalvar.RABBIT_OPEN_NOT_ALLOWED, '(this test uses open rabbit nodes)')
    def test_setting_and_resetting_nodes(self):

        # Make a node manager with several nodes, and remove them
        # one by one.
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.add_open_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.add_open_node(**TESTHELPERS.get_args_for_nodemanager())

        # Run code to be tested:
        # First, set one after the other, and check if the
        # number of URLs left is correct.
        # When all are used, reset the hosts and check if
        # the resetting worked fine.

        # We added five hosts, 3 trusted and 2 untrusted:
        self.assertEqual(mynodemanager.get_num_left_urls(),5)
        self.assertEqual(mynodemanager.get_num_left_trusted(),3)
        self.assertEqual(mynodemanager.get_num_left_open(),2)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the first host (1/5), 4 are left. (First, the trusted ones are selected):
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_num_left_urls(),4)
        self.assertEqual(mynodemanager.get_num_left_trusted(),2)
        self.assertEqual(mynodemanager.get_num_left_open(),2)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the second host (2/5), 3 are left.
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_num_left_urls(),3)
        self.assertEqual(mynodemanager.get_num_left_trusted(),1)
        self.assertEqual(mynodemanager.get_num_left_open(),2)
        # Set the third host (3/5), 2 are left (the two untrusted ones):
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_num_left_urls(),2)
        self.assertEqual(mynodemanager.get_num_left_trusted(),0)
        self.assertEqual(mynodemanager.get_num_left_open(),2)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the fourth host (4/5), 1 is left:
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_num_left_urls(),1)
        self.assertEqual(mynodemanager.get_num_left_trusted(),0)
        self.assertEqual(mynodemanager.get_num_left_open(),1)
        self.assertTrue(mynodemanager.has_more_urls())
        # Set the last host (5/5), so 0 are left:
        mynodemanager.set_next_host()
        self.assertEqual(mynodemanager.get_num_left_urls(),0)
        self.assertEqual(mynodemanager.get_num_left_trusted(),0)
        self.assertEqual(mynodemanager.get_num_left_open(),0)
        self.assertFalse(mynodemanager.has_more_urls())
        # Reset all nodes.
        # This includes the first call to set_next_host(), so it 
        # sets the first of five, so 4 are left:
        mynodemanager.reset_nodes()
        self.assertEqual(mynodemanager.get_num_left_urls(),4)
        self.assertEqual(mynodemanager.get_num_left_trusted(),2)
        self.assertEqual(mynodemanager.get_num_left_open(),2)
        self.assertTrue(mynodemanager.has_more_urls())

    '''
    Test getter for the routing key suffix (1/3).
    '''
    @unittest.skipIf(globalvar.RABBIT_OPEN_NOT_ALLOWED, '(this test uses open rabbit nodes)')
    def test_adapting_routing_key_open_only(self):

        # Test variables:
        # Make a node manager with only open nodes
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_open_node(**self.__get_args_dict())
        mynodemanager.add_open_node(**self.__get_args_dict())
        # Pre-check:
        self.assertEqual(mynodemanager.get_num_left_trusted(),0, 'Failing precheck.')
        self.assertEqual(mynodemanager.get_num_left_open(),2, 'Failing precheck.')
        # A node has to be set currently for this to work:
        mynodemanager.set_next_host()
        #print(str(mynodemanager._NodeManager__current_node))

        # Run code to be tested:
        routing_key = 'foo.fresh.foo'
        routing_key = mynodemanager.adapt_routing_key_for_untrusted(routing_key)

        # Check result:
        # Check if adapt_routing_key_for_untrusted() returns
        # the correct routing key. If there is only open
        # nodes, it must indicate that it's open nodes only.
        self.assertEqual('foo.fresh-untrusted-only.foo', routing_key)


    '''
    Test getter for the routing key suffix (2/3).
    '''
    def test_adapting_routing_key_trusted_only(self):

        # Test variables:
        # Make a node manager with only open nodes
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        # Pre-check:
        self.assertEqual(mynodemanager.get_num_left_trusted(),2, 'Failing precheck.')
        self.assertEqual(mynodemanager.get_num_left_open(),0, 'Failing precheck.')
        # A node has to be set currently for this to work:
        mynodemanager.set_next_host()
        #print(str(mynodemanager._NodeManager__current_node))

        # Run code to be tested
        routing_key = 'foo.fresh.foo'
        routing_key = mynodemanager.adapt_routing_key_for_untrusted(routing_key)

        # Check result:
        # Check if adapt_routing_key_for_untrusted() returns
        # the correct routing key.
        # If there is trusted nodes, and a trusted node is
        # currently set, it must not change the routing key.
        self.assertEqual('foo.fresh.foo', routing_key)


    '''
    Test getter for parameters for current host.
    '''
    @unittest.skipIf(globalvar.RABBIT_OPEN_NOT_ALLOWED, '(this test uses open rabbit nodes)')
    def test_get_params(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager(host='foo'))
        mynodemanager.add_open_node(**TESTHELPERS.get_args_for_nodemanager(host='bar'))

        # Run code to be tested (1/2):
        #mynodemanager.set_next_host() # This is called by the getter!!
        params = mynodemanager.get_connection_parameters()

        # Check result (1/2):
        self.assertIsInstance(params, pika.ConnectionParameters)
        self.assertEqual(params.host, 'foo')

        # Run code to be tested (2/2):
        mynodemanager.set_next_host()
        params = mynodemanager.get_connection_parameters()

        # Check result (2/2):
        self.assertIsInstance(params, pika.ConnectionParameters)
        self.assertEqual(params.host, 'bar')


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



    '''
    Test setting the priority to the lowest value, once.
    '''
    def test_change_prio(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.set_next_host()

        # Run code to be tested:
        old_prio = mynodemanager._get_prio_where_current_is_stored()
        mynodemanager.set_priority_low_for_current()
        new_prio = mynodemanager._get_prio_where_current_is_stored()

        # Check result
        self.assertEqual(old_prio, 'zzz_default', 'Default prio is %s, expected zzz_default' % old_prio)
        self.assertEqual(new_prio, 'zzzz_last',   'Prio after changing prio is %s, expected zzzz_last' % new_prio)



    '''
    Test setting the priority to the lowest value, once.
    Check if the password is masked from log output.
    '''
    def test_change_prio_pw_masking(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.set_next_host()

        # Run code to be tested:
        with self.assertLogs() as context_manager:
            mynodemanager.set_priority_low_for_current()

        # Check result
        self.assertTrue('pw_foo' not in str(context_manager.output), 'Password (pw_foo) is not masked: %s' % context_manager.output)
        self.assertTrue('pw_' in str(context_manager.output), 'Masked password stub (pw_f) missing: %s' % context_manager.output)


    '''
    Test setting the priority to the lowest value, twice.
    This tests whether the function can handle nodes that "already"
    have been set to the lowest prio.
    '''
    def test_change_prio_twice(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.set_next_host()

        # Run code to be tested:
        old_prio = mynodemanager._get_prio_where_current_is_stored()
        mynodemanager.set_priority_low_for_current()
        new_prio1 = mynodemanager._get_prio_where_current_is_stored()
        mynodemanager.set_priority_low_for_current()
        new_prio2 = mynodemanager._get_prio_where_current_is_stored()

        # Check result
        new_prio = mynodemanager._NodeManager__current_node['priority']
        self.assertEqual(old_prio, 'zzz_default', 'Default prio is %s, expected zzz_default' % old_prio)
        self.assertEqual(new_prio1, 'zzzz_last',   'Prio after changing prio is %s, expected zzzz_last' % new_prio1)
        self.assertEqual(new_prio2, 'zzzz_last',   'Prio after changing prio is %s, expected zzzz_last' % new_prio2)

    '''
    Test setting the priority to a weird nonexisting value
    This is only to test the code that should catch this - I doubt
    that this could ever ever every happen.
    '''
    def test_change_prio_weird_prio(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        mynodemanager.add_trusted_node(**TESTHELPERS.get_args_for_nodemanager())
        mynodemanager.set_next_host()
        # Change the prio to a weird value, to check if the functions still work:
        mynodemanager._NodeManager__trusted_nodes_archive['dummy_prio'] = [mynodemanager._NodeManager__current_node]
        del mynodemanager._NodeManager__trusted_nodes_archive['zzz_default']

        # Run code to be tested:
        old_prio = mynodemanager._get_prio_where_current_is_stored()
        mynodemanager.set_priority_low_for_current()
        new_prio1 = mynodemanager._get_prio_where_current_is_stored()
        mynodemanager.set_priority_low_for_current()
        new_prio2 = mynodemanager._get_prio_where_current_is_stored()

        # Check result
        new_prio = mynodemanager._NodeManager__current_node['priority']
        self.assertEqual(old_prio, 'dummy_prio', 'Default prio is %s, expected dummy_prio' % old_prio)
        self.assertEqual(new_prio1, 'zzzz_last',  'Prio after changing prio is %s, expected zzzz_last' % new_prio1)
        self.assertEqual(new_prio2, 'zzzz_last',  'Prio after changing prio is %s, expected zzzz_last' % new_prio2)


import unittest
import mock
import logging
import json
import sys
import os
sys.path.append("..")
import esgfpid.rabbit

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class RabbitNodemanagerTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test ########## (%s)' % os.path.basename(__file__))

    def tearDown(self):
        LOGGER.info('#############################')

    def __get_args_dict(self):
        return dict(
            urls_fallback='www.rabbit.foo',
            url_preferred=None,
            exchange_name='exch',
            username='rogerRabbit',
            password='mySecretCarrotDream',
            test_publication=False
        )

    def __get_testrabbit_asyn(self):
        args = self.__get_args_dict()
        return esgfpid.rabbit.rabbit.RabbitMessageSender(**args)

    # Tests

    '''
    Simple test whether the constructor works.
    It takes no args at all.
    '''
    def test_constructor(self):

        # Run code to be tested:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()

        # Check result
        self.assertIsInstance(mynodemanager, esgfpid.rabbit.nodemanager.NodeManager)

    def test_add_one_trusted_node_no_prio_ok(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        args = dict(
            username='foox',
            password='barx',
            host='myfoox.bar',
            exchange_name='myexchangex'
        )

        # Run code to be tested:
        mynodemanager.add_trusted_node(**args)

        # Check whether the correct number of nodes is there:
        self.assertEquals(mynodemanager.get_num_left_open(),0)
        self.assertEquals(mynodemanager.get_num_left_trusted(),1)
        self.assertEquals(mynodemanager.get_num_left_urls(),1)

    def test_add_trusted_nodes_no_prio_ok(self):

        # Test variables:
        mynodemanager = esgfpid.rabbit.nodemanager.NodeManager()
        args1 = dict(
            username='foo1',
            password='bar1',
            host='myfoo1.bar',
            exchange_name='myexchange1'
        )
        args2 = dict(
            username='foo2',
            password='bar2',
            host='myfoo2.bar',
            exchange_name='myexchange2'
        )

        # Run code to be tested:
        mynodemanager.add_trusted_node(**args1)
        mynodemanager.add_trusted_node(**args2)

        # Check whether the correct number of nodes is there:
        self.assertEquals(mynodemanager.get_num_left_open(),0)
        self.assertEquals(mynodemanager.get_num_left_trusted(),2)
        self.assertEquals(mynodemanager.get_num_left_urls(),2)
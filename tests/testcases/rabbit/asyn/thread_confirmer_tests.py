import unittest
import mock
import logging
import copy
import os
import esgfpid.rabbit.asynchronous.thread_confirmer

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

UNCONFIRMED_TAGS = [1,2,3,4]
UNCONFIRMED_MESSAGES = {'1':'foo1', '2':'foo2', '3':'foo3', '4':'foo4'}

class ThreadConfirmerTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    def make_confirmer(self):
        confirmer = esgfpid.rabbit.asynchronous.thread_confirmer.Confirmer()
        confirmer._Confirmer__unconfirmed_delivery_tags = copy.copy(UNCONFIRMED_TAGS)
        confirmer._Confirmer__unconfirmed_messages_dict = copy.copy(UNCONFIRMED_MESSAGES)
        return confirmer

    # Tests

    #
    # Acks
    #

    def test_single_ack_ok(self):

        # Preparation:
        confirmer = self.make_confirmer()
                
        # Make a fake ack:
        method_frame = mock.MagicMock()
        method_frame.method.delivery_tag = 2
        method_frame.method.multiple = False
        method_frame.method.NAME = 'foo.ack'

        # Run code to be tested:
        confirmer.on_delivery_confirmation(method_frame)

        # Check result (delivery tags)
        unconf = confirmer._Confirmer__unconfirmed_delivery_tags
        expected_unconf = [1,3,4]
        self.assertEquals(unconf, expected_unconf,
            'Unconfirmed delivery tags: %s, expected %s' % (unconf, expected_unconf))
        # Check result (messages)
        unconf_dict = confirmer._Confirmer__unconfirmed_messages_dict
        expected_unconf_dict = {'1':'foo1', '3':'foo3', '4':'foo4'}
        self.assertEquals(unconf_dict, expected_unconf_dict,
            'Unconfirmed messages: %s, expected %s' % (unconf_dict, expected_unconf_dict))


    def test_multiple_ack_ok(self):

        # Preparation:
        confirmer = self.make_confirmer()
        
        # Make a fake ack:
        method_frame = mock.MagicMock()
        method_frame.method.delivery_tag = 2
        method_frame.method.multiple = True
        method_frame.method.NAME = 'foo.ack'

        # Run code to be tested:
        confirmer.on_delivery_confirmation(method_frame)

        # Check result (delivery tags)
        unconf = confirmer._Confirmer__unconfirmed_delivery_tags
        expected_unconf = [3,4]
        self.assertEquals(unconf, expected_unconf,
            'Unconfirmed delivery tags: %s, expected %s' % (unconf, expected_unconf))
        # Check result (messages)
        unconf_dict = confirmer._Confirmer__unconfirmed_messages_dict
        expected_unconf_dict = {'3':'foo3', '4':'foo4'}
        self.assertEquals(unconf_dict, expected_unconf_dict,
            'Unconfirmed messages: %s, expected %s' % (unconf_dict, expected_unconf_dict))

    #
    # Nacks
    #

    def test_single_nack_ok(self):

        # Preparation:
        confirmer = self.make_confirmer()
                
        # Make a fake ack:
        method_frame = mock.Mock()
        method_frame.method.delivery_tag = 2
        method_frame.method.multiple = False
        method_frame.method.NAME = 'foo.nack'

        # Run code to be tested:
        confirmer.on_delivery_confirmation(method_frame)

        # Check result (delivery tags)
        unconf = confirmer._Confirmer__unconfirmed_delivery_tags
        expected_unconf = [1,3,4]
        self.assertEquals(unconf, expected_unconf,
            'Unconfirmed delivery tags: %s, expected %s' % (unconf, expected_unconf))
        # Check result (messages)
        unconf_dict = confirmer._Confirmer__unconfirmed_messages_dict
        expected_unconf_dict = {'1':'foo1', '3':'foo3', '4':'foo4'}
        self.assertEquals(unconf_dict, expected_unconf_dict,
            'Unconfirmed messages: %s, expected %s' % (unconf_dict, expected_unconf_dict))
        # Check result (nacked)
        nacked = confirmer._Confirmer__nacked_messages
        exp = ['foo2']
        self.assertEquals(nacked, exp,
            'Nacked messages: %s, expected %s' % (nacked, exp))

    def test_multiple_nack_ok(self):

        # Preparation:
        confirmer = self.make_confirmer()
        
        # Make a fake ack:
        method_frame = mock.Mock()
        method_frame.method.delivery_tag = 2
        method_frame.method.multiple = True
        method_frame.method.NAME = 'foo.nack'

        # Run code to be tested:
        confirmer.on_delivery_confirmation(method_frame)
        nacked_copy = confirmer.get_copy_of_nacked()

        # Check result (delivery tags)
        unconf = confirmer._Confirmer__unconfirmed_delivery_tags
        expected_unconf = [3,4]
        self.assertEquals(unconf, expected_unconf,
            'Unconfirmed delivery tags: %s, expected %s' % (unconf, expected_unconf))
        # Check result (messages)
        unconf_dict = confirmer._Confirmer__unconfirmed_messages_dict
        expected_unconf_dict = {'3':'foo3', '4':'foo4'}
        self.assertEquals(unconf_dict, expected_unconf_dict,
            'Unconfirmed messages: %s, expected %s' % (unconf_dict, expected_unconf_dict))
        # Check result (nacked)
        nacked = confirmer._Confirmer__nacked_messages
        exp = ['foo1', 'foo2']
        self.assertEquals(nacked, exp,
            'Nacked messages: %s, expected %s' % (nacked, exp))
        # Check result (nacked)
        exp = ['foo1', 'foo2']
        self.assertEquals(nacked_copy, exp,
            'Nacked messages: %s, expected %s' % (nacked_copy, exp))

    #
    # During publish
    #

    def test_putting_messages_ok(self):

        # Preparation:
        confirmer = self.make_confirmer()

        # Run code to be tested:
        confirmer.put_to_unconfirmed_delivery_tags(100)
        confirmer.put_to_unconfirmed_messages_dict(100, 'foo100')

        # Check result (delivery tags)
        unconf = confirmer._Confirmer__unconfirmed_delivery_tags
        expected_unconf = [1,2,3,4,100]
        self.assertEquals(unconf, expected_unconf,
            'Unconfirmed delivery tags: %s, expected %s' % (unconf, expected_unconf))
        # Check result (messages)
        unconf_dict = confirmer._Confirmer__unconfirmed_messages_dict
        expected_unconf_dict = {'1':'foo1', '2':'foo2', '3':'foo3', '4':'foo4', '100':'foo100'}
        self.assertEquals(unconf_dict, expected_unconf_dict,
            'Unconfirmed messages: %s, expected %s' % (unconf_dict, expected_unconf_dict))

    #
    # Getting leftovers
    #

    def test_getting_leftovers_ok(self):

        # Preparation:
        confirmer = self.make_confirmer()

        # Run code to be tested:
        mylist = confirmer.get_unconfirmed_messages_as_list_copy()
        num = confirmer.get_num_unconfirmed()

        # Check result (delivery tags)
        expected_messages = ['foo1', 'foo2', 'foo3', 'foo4']
        self.assertEquals(set(mylist), set(expected_messages),
            'Leftover messages: %s, expected %s' % (mylist, expected_messages))
        self.assertEquals(len(mylist), 4)
        self.assertEquals(num, 4)

    #
    # Reset
    #
 
    def test_reset_ok(self):
    
        # Preparation:
        confirmer = self.make_confirmer()
        
        # Run code to be tested:
        confirmer.reset_unconfirmed_messages_and_delivery_tags()
        unconf_retrieved = confirmer.get_copy_of_unconfirmed_tags()
    
        # Check result (delivery tags)
        unconf = confirmer._Confirmer__unconfirmed_delivery_tags
        self.assertEquals(unconf, [],
            'Unconfirmed delivery tags: %s, expected %s' % (unconf, []))
        self.assertEquals(unconf_retrieved, [],
            'Unconfirmed delivery tags: %s, expected %s' % (unconf_retrieved, []))

    #
    # Error
    #

    def test_neither_ack_nor_nack_error(self):

        # Preparation:
        confirmer = self.make_confirmer()
                
        # Make a fake ack:
        method_frame = mock.MagicMock()
        method_frame.method.delivery_tag = 2
        method_frame.method.multiple = False
        method_frame.method.NAME = 'foo.foo'

        # Run code to be tested:
        with self.assertRaises(esgfpid.rabbit.asynchronous.exceptions.UnknownServerResponse) as e:
            confirmer.on_delivery_confirmation(method_frame)
        self.assertIn('unknown type', e.exception.message)

    def test_server_response_error_1(self):

        # Preparation:
        confirmer = self.make_confirmer()
                
        # Make a fake ack:
        method_frame = mock.MagicMock()
        method_frame.method.delivery_tag = 2
        method_frame.method.multiple = False
        method_frame.method.NAME = 'foo' # has no dot!

        # Run code to be tested:
        with self.assertRaises(esgfpid.rabbit.asynchronous.exceptions.UnknownServerResponse) as e:
            confirmer.on_delivery_confirmation(method_frame)
        self.assertIn('list index out of range', e.exception.message)

    def test_server_response_error_2(self):

        # Preparation:
        confirmer = self.make_confirmer()
                
        # Make a fake ack:
        method_frame = mock.MagicMock()
        method_frame.method.delivery_tag = 2
        method_frame.method.multiple = False
        method_frame.method.NAME = None # is no string!

        # Run code to be tested:
        with self.assertRaises(esgfpid.rabbit.asynchronous.exceptions.UnknownServerResponse) as e:
            confirmer.on_delivery_confirmation(method_frame)
        self.assertIn('\'NoneType\' object has no attribute \'split\'', e.exception.message)

 
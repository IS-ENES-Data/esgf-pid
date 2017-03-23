import unittest
import mock
import logging
import json
import sys
import esgfpid.assistant.publish
from esgfpid.exceptions import ArgumentError
import tests.utils
from tests.utils import compare_json_return_errormessage as error_message
import tests.mocks.rabbitmock
import tests.mocks.solrmock
from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS
import esgfpid.utils

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import TESTVALUES
from resources.TESTVALUES import TEST_RABBIT_CREDS_OPEN
from resources.TESTVALUES import TEST_RABBIT_CREDS_TRUSTED
from resources.TESTVALUES import CONNECTOR_ARGS_MIN

# When we allow open nodes, the expected behaviour changes.
# So here is a flag, so we don't have to rewrite the tests
# every time:
OPEN_ALLOWED = False

class ApiTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)
        self.make_mocks()

    def tearDown(self):
        LOGGER.info('#############################')

    '''
    Mocks to avoid connection attempts.
    '''
    def make_mocks(self):
        self.default_solrmock = mock.MagicMock()
        self.default_solrmock.is_switched_off.return_value = False
        self.default_rabbitmock = mock.MagicMock()

    '''
    Get the minimum arguments a connector needs, add your own (as kwargs),
    so we don't have to repeat them all the time, or change every test
    case when the mandatory args change.
    '''
    def get_connector_args(self, **kwargs):
        connector_args = dict()
        connector_args.update(CONNECTOR_ARGS_MIN)
        for k,v in kwargs.iteritems():
            connector_args[k] = v
        return connector_args

    '''
    Get a connector object with the minimal arguments,
    plus your own (as kwargs).
    '''
    def get_connector(self, **kwargs):
        connector_args = self.get_connector_args(**kwargs)
        return esgfpid.Connector(**connector_args)

    '''
    Get a connector object with the minimal arguments,
    plus your own (as kwargs),
    and patched to avoid conenction attempts.
    '''
    def get_connector_patched(self, **kwargs):
        testconnector = self.get_connector(**kwargs)

        # Replace objects that interact with servers with mocks
        self.__patch_connector_with_solr_mock(testconnector)
        self.__patch_connector_with_rabbit_mock(testconnector)
        return testconnector

    def __patch_connector_with_solr_mock(self, connector, solrmock=None):
        if solrmock is None:
            solrmock = self.default_solrmock
        connector._Connector__coupler._Coupler__solr_sender = solrmock

    def __patch_connector_with_rabbit_mock(self, connector, rabbitmock=None):
        if rabbitmock is None:
            rabbitmock = self.default_rabbitmock
        connector._Connector__coupler._Coupler__rabbit_message_sender = rabbitmock

    # Actual tests:

    #
    # Init
    #

    '''
    Test the constructor, with trusted (and open) node.

    If open nodes are allowed, this should work fine.
    Otherwise, we expect an exception.
    '''
    def test_init_trusted_and_open_ok(self):

        # Preparations: Connector args.
        # Use trusted and open node:
        rabbit_creds = [TEST_RABBIT_CREDS_TRUSTED, TEST_RABBIT_CREDS_OPEN]
        args = self.get_connector_args(
            messaging_service_credentials = rabbit_creds
        )

        if OPEN_ALLOWED:

            # Run code to be tested: Connector constructior
            testconnector = esgfpid.Connector(**args)

            # Check result: Did init work?
            self.assertIsInstance(testconnector, esgfpid.Connector)

            # Check result:  Did the module get the right number of
            # trusted and open rabbit nodes?
            node_manager = testconnector._Connector__coupler._Coupler__rabbit_message_sender._RabbitMessageSender__node_manager
            self.assertEquals(node_manager.get_num_left_trusted(), 1)
            self.assertEquals(node_manager.get_num_left_open(),1)

        else:

            # Run code to be tested and check exception:
            with self.assertRaises(ArgumentError):
                testconnector = esgfpid.Connector(**args)

    def test_init_no_prefix(self):

        # Preparations: Connector args.
        # Use trusted and open node:
        rabbit_creds = [TEST_RABBIT_CREDS_TRUSTED]
        args = self.get_connector_args(
            messaging_service_credentials = rabbit_creds,
            handle_prefix = None
        )

        # Run code to be tested and check exception:
        with self.assertRaises(ArgumentError):
            testconnector = esgfpid.Connector(**args)

    def test_init_wrong_prefix(self):

        # Preparations: Connector args.
        # Use trusted and open node:
        rabbit_creds = [TEST_RABBIT_CREDS_TRUSTED]
        args = self.get_connector_args(
            messaging_service_credentials = rabbit_creds,
            handle_prefix = '987654321'
        )

        # Run code to be tested and check exception:
        with self.assertRaises(ArgumentError):
            testconnector = esgfpid.Connector(**args)


    def test_init_no_rabbit_url(self):

        # Preparations: Connector args.
        # Use trusted and open node:
        rabbit_creds = dict(
            user = TESTVALUES['rabbit_username_trusted'],
            password = TESTVALUES['rabbit_password']
        )
        args = self.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested and check exception:
        with self.assertRaises(ArgumentError):
            testconnector = esgfpid.Connector(**args)

    def test_init_no_rabbit_user(self):

        # Preparations: Connector args.
        # Use trusted and open node:
        rabbit_creds = dict(
            url = TESTVALUES['rabbit_url_trusted'],
            password = TESTVALUES['rabbit_password']
        )
        args = self.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested and check exception:
        with self.assertRaises(ArgumentError):
            testconnector = esgfpid.Connector(**args)

    '''
    Test the constructor, with trusted node.
    '''
    def test_init_trusted_only_ok(self):

        # Preparation: Connector args.
        # Use trusted node:
        rabbit_creds = [TEST_RABBIT_CREDS_TRUSTED]
        args = self.get_connector_args(
            messaging_service_credentials = rabbit_creds
        )

        # Run code to be tested: Connector constructor
        testconnector = esgfpid.Connector(**args)

        # Check results: Did init work?
        self.assertIsInstance(testconnector, esgfpid.Connector)

        # Check results: Did the module get the right number of
        # trusted and open rabbit nodes?
        node_manager = testconnector._Connector__coupler._Coupler__rabbit_message_sender._RabbitMessageSender__node_manager
        self.assertEquals(node_manager.get_num_left_trusted(), 1)
        self.assertEquals(node_manager.get_num_left_open(),0)

    '''
    Test the constructor, with only open nodes.
    '''
    def test_init_open_ok(self):

        # Preparation: Connector args.
        # Use open node:
        rabbit_creds = [TEST_RABBIT_CREDS_OPEN]
        args = self.get_connector_args(
            messaging_service_credentials = rabbit_creds
        )

        if OPEN_ALLOWED:

            # Run code to be tested: Connector constructor
            testconnector = esgfpid.Connector(**args)

            # Check results: Did init work?
            self.assertIsInstance(testconnector, esgfpid.Connector)

            # Check results: Did the module get the right number of
            # trusted and open rabbit nodes?
            node_manager = testconnector._Connector__coupler._Coupler__rabbit_message_sender._RabbitMessageSender__node_manager
            self.assertEquals(node_manager.get_num_left_trusted(), 0)
            self.assertEquals(node_manager.get_num_left_open(),1)

        else:

            # Run code to be tested and check exception:
            with self.assertRaises(ArgumentError):
                testconnector = esgfpid.Connector(**args)

    '''
    Test if the solr URL is passed to the consumer in the
    message.
    (The client wants to tell the consumer which solr
    instance to use for looking up dataset versions.
    Consumer has a default, if it is not passed. Consumer
    does not necessarily use solr, it might also use the
    handle database.)
    '''
    def test_passing_consumer_solr_url_ok(self):

        # Run code to be tested: Create connector with consumer solr url:
        # (And with the two necessary args for dataset publication)
        args = self.get_connector_args(
            consumer_solr_url='fake_solr_whatever',
            thredds_service_path='foo',
            data_node='bar'
        )
        testconnector = esgfpid.Connector(**args)

        # Run code to be tested: Init dataset wizard
        wizard = testconnector.create_publication_assistant(
            drs_id='baz',
            version_number=2016,
            is_replica=False
        )

        # Check result: Did init work?
        self.assertIsInstance(testconnector, esgfpid.Connector)
        self.assertIsInstance(wizard, esgfpid.assistant.publish.DatasetPublicationAssistant)

        # Check result: Was the consumer solr url passed?
        self.assertIsNotNone(testconnector._Connector__consumer_solr_url)
        self.assertIsNotNone(wizard._DatasetPublicationAssistant__consumer_solr_url)

    #
    # Publication
    #

    '''
    This passes the correct arguments.
    '''
    def test_create_publication_assistant_ok(self):

        # Preparations: Create connector with data node and thredds:
        args = self.get_connector_args(
            thredds_service_path='foo',
            data_node='bar'
        )
        testconnector = esgfpid.Connector(**args)

        # Run code to be tested: Init dataset wizard
        wizard = testconnector.create_publication_assistant(
            drs_id='baz',
            version_number=2016,
            is_replica=False
        )

        # Check result:
        self.assertIsInstance(wizard, esgfpid.assistant.publish.DatasetPublicationAssistant)


    '''
    If we want to publish a dataset, "data_node" and
    "thredds_service_path" have to be specified in the beginning!
    '''
    def test_create_publication_assistant_missing_thredds(self):

        # Preparations: Make connector without thredds:
        args = self.get_connector_args(data_node = 'foo')
        testconnector = esgfpid.Connector(**args)

        # Run code to be tested and check exception: Init dataset wizard
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            wizard = testconnector.create_publication_assistant(
                drs_id='bar',
                version_number=2016,
                is_replica=False
            )

    '''
    If we want to publish a dataset, "data_node" and
    "thredds_service_path" have to be specified in the beginning!
    '''
    def test_create_publication_assistant_missing_datanode(self):

        # Preparations: Make connector without thredds:
        args = self.get_connector_args(thredds_service_path = 'foo')
        testconnector = esgfpid.Connector(**args)

        # Run code to be tested and check exception: Init dataset wizard
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            wizard = testconnector.create_publication_assistant(
                drs_id='bar',
                version_number=2016,
                is_replica=False
            )

    #
    # Unpublication
    #

    '''
    If we want to unpublish a dataset, "data_node" has to
    be specified in the beginning!
    '''
    def test_unpublish_one_version_missing_data_node(self):

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        data_node = TESTVALUES['data_node']

        # Preparations: Make patched connector without the
        # necessary data node (needed for unpublish)
        default_testconnector = self.get_connector_patched()
  
        # Run code to be tested: Unpublish
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            default_testconnector.unpublish_one_version(
                drs_id=drs_id,
                version_number=version_number
            )

    '''
    If we want to unpublish a dataset, "data_node" has to
    be specified in the beginning!
    '''
    def test_unpublish_all_versions_missing_data_node(self):

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        data_node = TESTVALUES['data_node']

        # Preparations: Make patched connector without the
        # necessary data node (needed for unpublish)
        default_testconnector = self.get_connector_patched()
  
        # Run code to be tested: Unpublish
        with self.assertRaises(esgfpid.exceptions.ArgumentError):
            default_testconnector.unpublish_all_versions(drs_id=drs_id)

    '''
    This passes the correct args.
    '''
    def test_unpublish_one_version_ok(self):

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        data_node = TESTVALUES['data_node']

        # Preparations: Make patched connector with data node (needed for unpublish)
        default_testconnector = self.get_connector_patched(data_node=data_node)
  
        # Run code to be tested: Unpublish
        default_testconnector.unpublish_one_version(
            drs_id=drs_id,
            version_number=version_number
        )

        # Check result:
        expected_rabbit_task = {
            "handle": "hdl:"+TESTVALUES['prefix']+'/afd65cd0-9296-35bc-a706-be98665c9c36',
            "operation": "unpublish_one_version",
            "message_timestamp":"anydate",
            "aggregation_level":"dataset",
            "data_node": TESTVALUES['data_node'],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id":drs_id,
            "version_number": int(version_number)
        }
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    '''
    We unpublish one version.

    For this, no solr url is needed, as the version is already
    specified. So the consumer_solr_url is not passed on.
    '''
    def test_unpublish_one_version_with_consumer_url_ok(self):

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        data_node = TESTVALUES['data_node']

        # Preparations: Make connector, but without
        # solr mock. This is to see that with consumer_solr_url,
        # the library does not try to access solr.
        args = self.get_connector_args(
            consumer_solr_url='fake_solr_whatever',
            data_node=data_node
        )
        testconnector = esgfpid.Connector(**args)
        self.__patch_connector_with_rabbit_mock(testconnector)
  
        # Run code to be tested: Unpublish one version
        testconnector.unpublish_one_version(
            drs_id=drs_id,
            version_number=version_number
        )            

        # Check result:
        # We don't get the consumer_solr_url, because it is only
        # needed for unpublishing all versions.
        expected_rabbit_task = {
            "handle": "hdl:"+TESTVALUES['prefix']+'/afd65cd0-9296-35bc-a706-be98665c9c36',
            "operation": "unpublish_one_version",
            "message_timestamp":"anydate",
            "aggregation_level":"dataset",
            "data_node": TESTVALUES['data_node'],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id":drs_id,
            "version_number": int(version_number)
        }
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    '''
    Test unpublishing all versions.

    For this, the library would call solr to find handles of
    all versions. We make our mock solr return None, so the
    library must tell the consumer to look for the versions
    itself.

    This time, we tell the consumer to use its default solr url.
    '''
    @mock.patch('esgfpid.coupling.Coupler.retrieve_datasethandles_or_versionnumbers_of_allversions')
    def test_unpublish_all_versions_nosolr_ok(self, solr_asker_patch):

        # Patch: Make the coupler return None when we ask it
        # to find dataset versions...
        mydict = dict(dataset_handles=None, version_numbers=None)
        solr_asker_patch.return_value = mydict

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        data_node = TESTVALUES['data_node']

        # Preparations: Create patched connector
        default_testconnector = self.get_connector_patched(data_node=data_node)

        # Run code to be tested:
        default_testconnector.unpublish_all_versions(drs_id=drs_id)

        # Check result:
        expected_rabbit_task = {
            "operation": "unpublish_all_versions",
            "message_timestamp": "anydate",
            "data_node": data_node,
            "aggregation_level":"dataset",
            "drs_id":drs_id,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all'
        }
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    def test_unpublish_all_versions_solr_off(self):

        # Test variables
        drs_id = TESTVALUES['drs_id1']
        data_node = TESTVALUES['data_node']

        # Preparations: Create patched connector
        testconnector = self.get_connector(
            data_node=data_node,
            solr_switched_off=True 
        )
        self.__patch_connector_with_rabbit_mock(testconnector)
        testconnector.unpublish_all_versions(drs_id=drs_id)

        # Check result:
        expected_rabbit_task = {
            "operation": "unpublish_all_versions",
            "message_timestamp": "anydate",
            "data_node": data_node,
            "aggregation_level":"dataset",
            "drs_id":drs_id,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all'
        }
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))


    '''
    Test unpublishing all versions.


    For this, the library would call solr to find handles of
    all versions. We make our mock solr return None, so the
    library must tell the consumer to look for the versions
    itself.

    This time, we tell the consumer to use the solr url
    we provide.
    '''
    @mock.patch('esgfpid.coupling.Coupler.retrieve_datasethandles_or_versionnumbers_of_allversions')
    def test_unpublish_all_versions_nosolr__butconsumersolr_ok(self, solr_asker_patch):

        # Patch coupler
        mydict = dict(dataset_handles=None, version_numbers=None)
        solr_asker_patch.return_value = mydict

        # Test variables:
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        data_node = TESTVALUES['data_node']
        library_solr_url = TESTVALUES['solr_url']
        consumer_solr_url = 'fake_solr_whatever'

        # Preparations: Create patched connector
        args = self.get_connector_args(
            data_node = data_node,
            solr_url = library_solr_url,
            consumer_solr_url=consumer_solr_url
        )
        testconnector = esgfpid.Connector(**args)
        self.__patch_connector_with_rabbit_mock(testconnector)

        # Run code to be tested:
        testconnector.unpublish_all_versions(
            drs_id=drs_id,
            data_node=data_node)

        # Check result:
        expected_rabbit_task = {
            "operation": "unpublish_all_versions",
            "message_timestamp": "anydate",
            "data_node": data_node,
            "aggregation_level":"dataset",
            "drs_id":drs_id,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all',
            "consumer_solr_url":consumer_solr_url
        }        
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    #
    # Errata
    #

    def test_add_errata_id_several_ok(self):

        # Test variables:
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        errata_ids = TESTVALUES['errata_ids']

        # Preparations: Create patched connector
        # (Patched to avoid that message be sent, and to retrieve the created message)
        default_testconnector = self.get_connector_patched()

        # Run code to be tested: Add errata
        default_testconnector.add_errata_ids(
            drs_id=drs_id,
            version_number=version_number,
            errata_ids=errata_ids
        )

        # Check result: Was correct errata message created?
        expected_rabbit_task = {
            "handle": "hdl:"+TESTVALUES['prefix']+"/afd65cd0-9296-35bc-a706-be98665c9c36",
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":errata_ids,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":drs_id,
            "version_number":version_number
        }
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    def test_add_errata_id_one_ok(self):

        # Test variables:
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        errata_id = TESTVALUES['errata_id']

        # Preparations: Create patched connector
        # (Patched to avoid that message be sent, and to retrieve the created message)
        default_testconnector = self.get_connector_patched()

        # Run code to be tested: Add errata
        default_testconnector.add_errata_ids(
            drs_id=drs_id,
            version_number=version_number,
            errata_ids=errata_id
        )

        # Check result: Was correct errata message created?
        expected_rabbit_task = {
            "handle": "hdl:"+TESTVALUES['prefix']+"/afd65cd0-9296-35bc-a706-be98665c9c36",
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":[errata_id],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":drs_id,
            "version_number":version_number
        }
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))

    def test_remove_errata_id_one_ok(self):

        # Test variables:
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        errata_id = TESTVALUES['errata_id']

        # Preparations: Create patched connector
        # (Patched to avoid that message be sent, and to retrieve the created message)
        default_testconnector = self.get_connector_patched()

        # Run code to be tested: Add errata
        default_testconnector.remove_errata_ids(
            drs_id=drs_id,
            version_number=version_number,
            errata_ids=errata_id
        )

        # Check result: Was correct errata message created?
        expected_rabbit_task = {
            "handle": "hdl:"+TESTVALUES['prefix']+"/afd65cd0-9296-35bc-a706-be98665c9c36",
            "operation": "remove_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":[errata_id],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.remove',
            "drs_id":drs_id,
            "version_number":version_number
        }
        received_rabbit_task = self.default_rabbitmock.send_message_to_queue.call_args[0][0] # first get positional args, second get the first og those
        tests.utils.replace_date_with_string(received_rabbit_task)
        is_same = tests.utils.is_json_same(expected_rabbit_task, received_rabbit_task)
        self.assertTrue(is_same, tests.utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_task))


    #
    # Data Cart
    #

    def test_make_data_cart_pid(self):

        # Test variables
        prefix = TESTVALUES['prefix']
        content1 = {'foo':'foo', 'bar':'bar'}
        content2 = {'foo':'foo', 'bar': None}

        # Preparations: Create patched connector
        # (Patched to avoid that message be sent, and to retrieve the created message)
        testconnector = self.get_connector_patched()

        # Run code to be tested: Create data cart PIDs
        # And retrieve the messages
        pid1 = testconnector.create_data_cart_pid(content1)
        received_rabbit_task1 = self.default_rabbitmock.send_message_to_queue.call_args[0][0]
        pid2 = testconnector.create_data_cart_pid(content2)
        received_rabbit_task2 = self.default_rabbitmock.send_message_to_queue.call_args[0][0]

        # Check result: Were the correct messages created?
        expected_handle_both_cases = "hdl:"+prefix+"/b597a79e-1dc7-3d3f-b689-75ac5a78167f"
        expected_rabbit_task1 = {
            "handle": expected_handle_both_cases,
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":content1,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        expected_rabbit_task2 = {
            "handle": expected_handle_both_cases,
            "operation": "shopping_cart",
            "message_timestamp":"anydate",
            "data_cart_content":content2,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'cart.datasets'
        }
        tests.utils.replace_date_with_string(received_rabbit_task1)
        tests.utils.replace_date_with_string(received_rabbit_task2)
        same1 = tests.utils.is_json_same(expected_rabbit_task1, received_rabbit_task1)
        same2 = tests.utils.is_json_same(expected_rabbit_task2, received_rabbit_task2)
        self.assertTrue(same1, error_message(expected_rabbit_task1, received_rabbit_task1))
        self.assertTrue(same2, error_message(expected_rabbit_task2, received_rabbit_task2))
        self.assertTrue(pid1==pid2, 'Both pids are not the same.')

    #
    # Threads
    #

    def test_start_messaging_thread(self):
        LOGGER.debug('Thread test')
        default_testconnector = self.get_connector_patched()
        default_testconnector.start_messaging_thread()
        self.default_rabbitmock.start.assert_called_with()

    def test_finish_messaging_thread(self):
        LOGGER.debug('Thread test')
        default_testconnector = self.get_connector_patched()
        default_testconnector.finish_messaging_thread()
        self.default_rabbitmock.finish.assert_called_with()

    def test_force_finish_messaging_thread(self):
        LOGGER.debug('Thread test')
        default_testconnector = self.get_connector_patched()
        default_testconnector.force_finish_messaging_thread()
        self.default_rabbitmock.force_finish.assert_called_with()

    def test_make_handle(self):
        testconnector = self.get_connector()
        drs_id = TESTVALUES['drs_id1']
        version_number = TESTVALUES['version_number1']
        received_handle = testconnector.make_handle_from_drsid_and_versionnumber(
            drs_id=drs_id,
            version_number=version_number
        )
        expected_handle = 'hdl:'+TESTVALUES['prefix']+'/afd65cd0-9296-35bc-a706-be98665c9c36'
        self.assertEquals(received_handle, expected_handle)
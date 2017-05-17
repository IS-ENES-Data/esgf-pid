import unittest
import mock
import logging
import tests.utils as utils
from tests.utils import compare_json_return_errormessage as error_message

# Import of code to be tested:
import esgfpid.assistant.publish
from esgfpid.exceptions import ArgumentError
from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS

# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

# Test resources:
from resources.TESTVALUES import *
import resources.TESTVALUES as TESTHELPERS
from resources.TESTVALUES import TEST_RABBIT_CREDS_OPEN
from resources.TESTVALUES import TEST_RABBIT_CREDS_TRUSTED

# Some tests rely on open nodes
import tests.globalvar
import globalvar
if globalvar.RABBIT_OPEN_NOT_ALLOWED:
    print('Skipping tests that need open RabbitMQ nodes in module "%s".' % __name__)

class ConnectorTestCase(unittest.TestCase):

    def setUp(self):
        LOGGER.info('######## Next test (%s) ##########', __name__)

    def tearDown(self):
        LOGGER.info('#############################')

    #
    # Init
    #

    '''
    Test the constructor, with trusted (and open) node.

    If open nodes are allowed, this should work fine.
    Otherwise, we expect an exception.
    '''
    @unittest.skipIf(not(globalvar.RABBIT_OPEN_NOT_ALLOWED), '(this test cannot cope with open rabbit nodes)')
    def test_init_trusted_and_open_ok_1(self):

        # Preparations: Connector args.
        # Use trusted and open node:
        rabbit_creds = [copy.deepcopy(TEST_RABBIT_CREDS_TRUSTED), copy.deepcopy(TEST_RABBIT_CREDS_OPEN)]
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = rabbit_creds
        )

        # Run code to be tested and check exception:
        with self.assertRaises(ArgumentError) as e:
            testconnector = esgfpid.Connector(**args)

        # Check result: Error message ok?
        self.assertIn('Missing password', str(e.exception))

    '''
    Test the constructor, with trusted (and open) node.

    If open nodes are allowed, this should work fine.
    Otherwise, we expect an exception.
    '''
    @unittest.skipIf(globalvar.RABBIT_OPEN_NOT_ALLOWED, '(this test uses open rabbit nodes)')
    def test_init_trusted_and_open_ok_2(self):

        # Preparations: Connector args.
        # Use trusted and open node:
        rabbit_creds = [copy.deepcopy(TEST_RABBIT_CREDS_TRUSTED), copy.deepcopy(TEST_RABBIT_CREDS_OPEN)]
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = rabbit_creds
        )

        # Run code to be tested: Connector constructior
        testconnector = esgfpid.Connector(**args)

        # Check result: Did init work?
        self.assertIsInstance(testconnector, esgfpid.Connector)

        # Check result: Did the module get the right number of
        # trusted and open rabbit nodes?
        node_manager = testconnector._Connector__coupler._Coupler__rabbit_message_sender._RabbitMessageSender__node_manager
        self.assertEquals(node_manager.get_num_left_trusted(), 1)
        self.assertEquals(node_manager.get_num_left_open(),1)


    def test_init_no_prefix(self):

        # Preparations: Connector args.
        rabbit_creds = [copy.deepcopy(TEST_RABBIT_CREDS_TRUSTED)]
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = rabbit_creds,
            handle_prefix = None
        )

        # Run code to be tested and check exception:
        with self.assertRaises(ArgumentError) as e:
            testconnector = esgfpid.Connector(**args)

        # Check result: Error message ok?
        self.assertIn('Missing handle prefix', str(e.exception))

    def test_init_wrong_prefix(self):

        # Preparations: Connector args.
        rabbit_creds = [copy.deepcopy(TEST_RABBIT_CREDS_TRUSTED)]
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = rabbit_creds,
            handle_prefix = '987654321'
        )

        # Run code to be tested and check exception:
        with self.assertRaises(ArgumentError) as e:
            testconnector = esgfpid.Connector(**args)

        # Check result: Error message ok?
        self.assertIn('is not a valid prefix', str(e.exception))

    def test_init_no_rabbit_url(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            user = RABBIT_USER_TRUSTED,
            password = RABBIT_PASSWORD
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested and check exception:
        with self.assertRaises(ArgumentError) as e:
            testconnector = esgfpid.Connector(**args)

        # Check result: Error message ok?
        self.assertIn('Missing url for', str(e.exception))

    def test_init_no_rabbit_user(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            url = RABBIT_URL_TRUSTED,
            password = RABBIT_PASSWORD
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested and check exception:
        with self.assertRaises(ArgumentError) as e:
            testconnector = esgfpid.Connector(**args)

        # Check result: Error message ok?
        self.assertIn('Missing user', str(e.exception))

    '''
    Test the constructor, with trusted node.
    '''
    def test_init_trusted_only_ok(self):

        # Preparation: Connector args.
        # Use trusted node:
        rabbit_creds = [copy.deepcopy(TEST_RABBIT_CREDS_TRUSTED)]
        args = TESTHELPERS.get_connector_args(
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
    Test the constructor, with trusted node.
    '''
    def test_init_trusted_only_more_args_ok(self):

        # Preparation: Connector args.
        # Use trusted node:
        rabbit_creds = TESTHELPERS.get_rabbit_credentials(vhost='foo', port=666, ssl_enabled=True)
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
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
        # Check: Were the right values passed?
        node_manager.set_next_host()
        curr = node_manager._NodeManager__current_node
        self.assertEquals(curr['vhost'],'foo')
        self.assertEquals(curr['port'],666)
        self.assertEquals(curr['ssl_enabled'],True)

    '''
    Test the constructor, with only open nodes.
    '''
    @unittest.skipIf(not(globalvar.RABBIT_OPEN_NOT_ALLOWED), '(this test cannot deal with open rabbit nodes)')
    def test_init_open_ok(self):

        # Preparation: Connector args.
        # Use open node:
        rabbit_creds = [copy.deepcopy(TEST_RABBIT_CREDS_OPEN)]
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = rabbit_creds
        )

        # Run code to be tested and check exception:
        with self.assertRaises(ArgumentError) as e:
            testconnector = esgfpid.Connector(**args)

        # Check result: Error message ok?
        self.assertIn('Missing password', str(e.exception))


    '''
    Test the constructor, with only open nodes.
    '''
    @unittest.skipIf(globalvar.RABBIT_OPEN_NOT_ALLOWED, '(this test uses open rabbit nodes)')
    def test_init_open_ok_2(self):

        # Preparation: Connector args.
        # Use open node:
        rabbit_creds = [copy.deepcopy(TEST_RABBIT_CREDS_OPEN)]
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = rabbit_creds
        )

        # Run code to be tested: Connector constructor
        testconnector = esgfpid.Connector(**args)

        # Check results: Did init work?
        self.assertIsInstance(testconnector, esgfpid.Connector)

        # Check results: Did the module get the right number of
        # trusted and open rabbit nodes?
        node_manager = testconnector._Connector__coupler._Coupler__rabbit_message_sender._RabbitMessageSender__node_manager
        self.assertEquals(node_manager.get_num_left_trusted(), 0)
        self.assertEquals(node_manager.get_num_left_open(),1)

    def test_init_rabbit_user_as_list(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            url = RABBIT_URL_TRUSTED,
            user = [RABBIT_USER_TRUSTED],
            password = RABBIT_PASSWORD
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested:
        testconnector = esgfpid.Connector(**args)

        # Check result: Did init work?
        self.assertIsInstance(testconnector, esgfpid.Connector)

    def test_init_too_many_rabbit_users(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            url = RABBIT_URL_TRUSTED,
            user = [RABBIT_USER_TRUSTED, 'johndoe', 'alicedoe'],
            password = RABBIT_PASSWORD
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested:
        with self.assertRaises(ArgumentError) as e:
            testconnector = esgfpid.Connector(**args)

        # Check result: Error message ok?
        self.assertIn('Wrong type', str(e.exception))

    def test_init_vhost_no_string(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            url = RABBIT_URL_TRUSTED,
            user = RABBIT_USER_TRUSTED,
            password = RABBIT_PASSWORD,
            vhost = 123
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested:
        with self.assertRaises(ArgumentError) as e:
            testconnector = esgfpid.Connector(**args)

        # Check result: Error message ok?
        self.assertIn('Wrong type', str(e.exception))

    def test_init_sslenabled_no_bool(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            url = RABBIT_URL_TRUSTED,
            user = RABBIT_USER_TRUSTED,
            password = RABBIT_PASSWORD,
            ssl_enabled = 123
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested:
        with self.assertRaises(ArgumentError) as e:
            testconnector = esgfpid.Connector(**args)

        # Check result: Error message ok?
        self.assertIn('Wrong type', str(e.exception))

    def test_init_sslenabled_string_bool_true(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            url = RABBIT_URL_TRUSTED,
            user = RABBIT_USER_TRUSTED,
            password = RABBIT_PASSWORD,
            ssl_enabled = 'tRuE'
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested:
        testconnector = esgfpid.Connector(**args)

        # Check result: Did init work?
        self.assertIsInstance(testconnector, esgfpid.Connector)
        # Check: Were the right values passed?
        node_manager = testconnector._Connector__coupler._Coupler__rabbit_message_sender._RabbitMessageSender__node_manager
        node_manager.set_next_host()
        curr = node_manager._NodeManager__current_node
        self.assertEquals(curr['ssl_enabled'],True)

    def test_init_sslenabled_string_bool_false(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            url = RABBIT_URL_TRUSTED,
            user = RABBIT_USER_TRUSTED,
            password = RABBIT_PASSWORD,
            ssl_enabled = 'fAlSe'
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested:
        testconnector = esgfpid.Connector(**args)

        # Check result: Did init work?
        self.assertIsInstance(testconnector, esgfpid.Connector)
        # Check: Were the right values passed?
        node_manager = testconnector._Connector__coupler._Coupler__rabbit_message_sender._RabbitMessageSender__node_manager
        node_manager.set_next_host()
        curr = node_manager._NodeManager__current_node
        self.assertEquals(curr['ssl_enabled'],False)

    def test_init_sslenabled_string_bool_other(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            url = RABBIT_URL_TRUSTED,
            user = RABBIT_USER_TRUSTED,
            password = RABBIT_PASSWORD,
            ssl_enabled = 'yes'
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested:
        with self.assertRaises(ArgumentError) as e:
            testconnector = esgfpid.Connector(**args)

        # Check result: Error message ok?
        self.assertIn('Wrong type', str(e.exception))

    def test_init_sslenabled_string_bool_other(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            url = RABBIT_URL_TRUSTED,
            user = RABBIT_USER_TRUSTED,
            password = RABBIT_PASSWORD,
            ssl_enabled = '',
            vhost = '',
            port = ''
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested:
        testconnector = esgfpid.Connector(**args)

        # Check result: Did init work?
        self.assertIsInstance(testconnector, esgfpid.Connector)
        # Check: Were the right values passed?
        node_manager = testconnector._Connector__coupler._Coupler__rabbit_message_sender._RabbitMessageSender__node_manager
        node_manager.set_next_host()
        curr = node_manager._NodeManager__current_node
        self.assertEquals(curr['ssl_enabled'],None)
        self.assertEquals(curr['vhost'],None)
        self.assertEquals(curr['port'],None)

    def test_init_port_no_int(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            url = RABBIT_URL_TRUSTED,
            user = RABBIT_USER_TRUSTED,
            password = RABBIT_PASSWORD,
            port = 'foo'
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested:
        with self.assertRaises(ArgumentError) as e:
            testconnector = esgfpid.Connector(**args)

        # Check result: Error message ok?
        self.assertIn('Wrong type', str(e.exception))

    def test_init_port_string_int(self):

        # Preparations: Connector args.
        rabbit_creds = dict(
            url = RABBIT_URL_TRUSTED,
            user = RABBIT_USER_TRUSTED,
            password = RABBIT_PASSWORD,
            port = '123'
        )
        args = TESTHELPERS.get_connector_args(
            messaging_service_credentials = [rabbit_creds]
        )

        # Run code to be tested:
        testconnector = esgfpid.Connector(**args)

        # Check result: Did init work?
        self.assertIsInstance(testconnector, esgfpid.Connector)
        # Check: Were the right values passed?
        node_manager = testconnector._Connector__coupler._Coupler__rabbit_message_sender._RabbitMessageSender__node_manager
        node_manager.set_next_host()
        curr = node_manager._NodeManager__current_node
        self.assertEquals(curr['ssl_enabled'],None)
        self.assertEquals(curr['vhost'],None)
        self.assertEquals(curr['port'],123)

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
        args = TESTHELPERS.get_connector_args(
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


    '''
    Test whether the correct defaults are set
    if the connector is initialized with the minimum
    arguments.

    '''
    def test_init_default_args_to_coupler(self):

        # Preparations: Minimum args for connector
        args = TESTHELPERS.get_connector_args()

        # Run code to be tested: Connector constructor
        testconnector = esgfpid.Connector(**args)

        # Check results: Check if the correct defaults
        # were set (i.e. passed to coupler.)
        coupler_args = testconnector._Connector__coupler.args
        self.assertEquals(coupler_args['data_node'],None)
        self.assertEquals(coupler_args['thredds_service_path'],None)
        self.assertEquals(coupler_args['test_publication'],False)
        self.assertEquals(coupler_args['solr_url'],None)
        self.assertEquals(coupler_args['solr_switched_off'],True)
        self.assertEquals(coupler_args['solr_https_verify'],False)
        self.assertEquals(coupler_args['disable_insecure_request_warning'],False)
        self.assertEquals(coupler_args['message_service_synchronous'],False)
        self.assertEquals(coupler_args['consumer_solr_url'],None)
        
    '''
    Test whether the correct defaults are set
    if the connector is initialized with the minimum
    arguments.
    '''
    def test_init_not_default_args_to_coupler(self):

        # Run code to be tested: Make a connector
        # with many non-default args:
        args = TESTHELPERS.get_connector_args(
            data_node=DATA_NODE,
            thredds_service_path=THREDDS,
            test_publication=True,
            solr_url=SOLR_URL_LIBRARY,
            solr_https_verify=True,
            disable_insecure_request_warning=True,
            message_service_synchronous=True,
            consumer_solr_url=SOLR_URL_CONSUMER
        )
        testconnector = esgfpid.Connector(**args)

        # Check results: Check if the correct defaults
        # were set (i.e. passed to coupler.)
        coupler_args = testconnector._Connector__coupler.args
        self.assertEquals(coupler_args['data_node'],DATA_NODE)
        self.assertEquals(coupler_args['thredds_service_path'],THREDDS)
        self.assertEquals(coupler_args['test_publication'],True)
        self.assertEquals(coupler_args['solr_url'],SOLR_URL_LIBRARY)
        self.assertEquals(coupler_args['solr_switched_off'],False)
        self.assertEquals(coupler_args['solr_https_verify'],True)
        self.assertEquals(coupler_args['disable_insecure_request_warning'],True)
        self.assertEquals(coupler_args['message_service_synchronous'],True)
        self.assertEquals(coupler_args['consumer_solr_url'],SOLR_URL_CONSUMER)

    '''
    Check if solr is not switched off if an URL given.
    '''
    def test_init_solr_not_off(self):

        # Run code to be tested: Make connector with solr url
        args = TESTHELPERS.get_connector_args(solr_url='foo')
        testconnector = esgfpid.Connector(**args)

        # Check results: Check if the correct defaults
        # were set (i.e. passed to coupler.)
        coupler_args = testconnector._Connector__coupler.args
        self.assertEquals(coupler_args['solr_url'],'foo')
        self.assertEquals(coupler_args['solr_switched_off'],False)

    '''
    Check if solr is not switched off if an URL given.
    '''
    def test_init_solr_off(self):

        # Preparations: Minimum args for connector
        args = TESTHELPERS.get_connector_args()

        # Run code to be tested: Connector constructor
        testconnector = esgfpid.Connector(**args)

        # Check results: Check if the correct defaults
        # were set (i.e. passed to coupler.)
        coupler_args = testconnector._Connector__coupler.args
        self.assertEquals(coupler_args['solr_url'],None)
        self.assertEquals(coupler_args['solr_switched_off'],True)


    #
    # Publication
    #

    '''
    This passes the correct arguments.
    '''
    def test_create_publication_assistant_ok(self):

        # Preparations: Create connector with data node and thredds:
        args = TESTHELPERS.get_connector_args(
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
        args = TESTHELPERS.get_connector_args(data_node = 'foo')
        testconnector = esgfpid.Connector(**args)

        # Run code to be tested and check exception: Init dataset wizard
        with self.assertRaises(esgfpid.exceptions.ArgumentError) as e:
            wizard = testconnector.create_publication_assistant(
                drs_id='bar',
                version_number=2016,
                is_replica=False
            )

        # Check result: Error message ok?
        self.assertIn('No thredds_service_path given', str(e.exception))

    '''
    If we want to publish a dataset, "data_node" and
    "thredds_service_path" have to be specified in the beginning!
    '''
    def test_create_publication_assistant_missing_datanode(self):

        # Preparations: Make connector without thredds:
        args = TESTHELPERS.get_connector_args(thredds_service_path = 'foo')
        testconnector = esgfpid.Connector(**args)

        # Run code to be tested and check exception: Init dataset wizard
        with self.assertRaises(esgfpid.exceptions.ArgumentError) as e:
            wizard = testconnector.create_publication_assistant(
                drs_id='bar',
                version_number=2016,
                is_replica=False
            )

        # Check result: Error message ok?
        self.assertIn('No data_node given', str(e.exception))

    #
    # Unpublication
    #

    '''
    If we want to unpublish a dataset, "data_node" has to
    be specified in the beginning!
    '''
    def test_unpublish_all_versions_missing_data_node(self):

        # Preparations: Make patched connector without the
        # necessary data node (needed for unpublish)
        testconnector = TESTHELPERS.get_connector()
        TESTHELPERS.patch_with_rabbit_mock(testconnector)

        # Run code to be tested: Unpublish
        with self.assertRaises(esgfpid.exceptions.ArgumentError) as e:
            testconnector.unpublish_all_versions(drs_id=DRS_ID)

        # Check result: Error message ok?
        self.assertIn('No data_node given', str(e.exception))

    '''
    This passes the correct args.
    '''
    def test_unpublish_one_version_ok(self):

        # Preparations: Make patched connector with data node (needed for unpublish)
        testconnector = TESTHELPERS.get_connector(data_node=DATA_NODE)
        TESTHELPERS.patch_with_rabbit_mock(testconnector)
  
        # Run code to be tested: Unpublish
        testconnector.unpublish_one_version(
            drs_id=DRS_ID,
            version_number=DS_VERSION
        )

        # Check result:
        expected_rabbit_task = {
            "handle": DATASETHANDLE_HDL,
            "operation": "unpublish_one_version",
            "message_timestamp":"anydate",
            "aggregation_level":"dataset",
            "data_node": DATA_NODE,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id":DRS_ID,
            "version_number": int(DS_VERSION)
        }
        received_rabbit_msg = TESTHELPERS.get_received_message_from_rabbitmock(testconnector)
        is_same = utils.is_json_same(expected_rabbit_task, received_rabbit_msg)
        self.assertTrue(is_same, utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_msg))

    '''
    We unpublish one version.

    For this, no solr url is needed, as the version is already
    specified. So the consumer_solr_url is not passed on.
    '''
    def test_unpublish_one_version_with_consumer_url_ok(self):

        # Preparations: Make connector, but without
        # solr mock. This is to see that with consumer_solr_url,
        # the library does not try to access solr.
        testconnector = TESTHELPERS.get_connector(
            consumer_solr_url=SOLR_URL_CONSUMER,
            data_node=DATA_NODE
        )
        TESTHELPERS.patch_with_rabbit_mock(testconnector)
  
        # Run code to be tested: Unpublish one version
        testconnector.unpublish_one_version(
            drs_id=DRS_ID,
            version_number=DS_VERSION
        )            

        # Check result:
        # We don't get the consumer_solr_url, because it is only
        # needed for unpublishing all versions.
        expected_rabbit_task = {
            "handle": DATASETHANDLE_HDL,
            "operation": "unpublish_one_version",
            "message_timestamp":"anydate",
            "aggregation_level":"dataset",
            "data_node": DATA_NODE,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
            "drs_id":DRS_ID,
            "version_number": int(DS_VERSION)
        }
        received_rabbit_msg = TESTHELPERS.get_received_message_from_rabbitmock(testconnector)
        is_same = utils.is_json_same(expected_rabbit_task, received_rabbit_msg)
        self.assertTrue(is_same, utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_msg))

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

        # Preparations: Create patched connector
        testconnector = TESTHELPERS.get_connector(data_node=DATA_NODE)
        TESTHELPERS.patch_with_rabbit_mock(testconnector)

        # Run code to be tested:
        testconnector.unpublish_all_versions(drs_id=DRS_ID)

        # Check result:
        expected_rabbit_task = {
            "operation": "unpublish_all_versions",
            "message_timestamp": "anydate",
            "data_node": DATA_NODE,
            "aggregation_level":"dataset",
            "drs_id":DRS_ID,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all'
        }
        received_rabbit_msg = TESTHELPERS.get_received_message_from_rabbitmock(testconnector)
        is_same = utils.is_json_same(expected_rabbit_task, received_rabbit_msg)
        self.assertTrue(is_same, utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_msg))

    def test_unpublish_all_versions_solr_off(self):

        # Preparations: Create patched connector
        testconnector = TESTHELPERS.get_connector(
            data_node=DATA_NODE,
            solr_switched_off=True 
        )
        TESTHELPERS.patch_with_rabbit_mock(testconnector)

        # Run code to be tested:
        testconnector.unpublish_all_versions(drs_id=DRS_ID)

        # Check result:
        expected_rabbit_task = {
            "operation": "unpublish_all_versions",
            "message_timestamp": "anydate",
            "data_node": DATA_NODE,
            "aggregation_level":"dataset",
            "drs_id":DRS_ID,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all'
        }
        received_rabbit_msg = TESTHELPERS.get_received_message_from_rabbitmock(testconnector)
        is_same = utils.is_json_same(expected_rabbit_task, received_rabbit_msg)
        self.assertTrue(is_same, utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_msg))


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

        # Preparations: Create patched connector
        testconnector = TESTHELPERS.get_connector(
            data_node = DATA_NODE,
            solr_url = SOLR_URL_LIBRARY,
            consumer_solr_url=SOLR_URL_CONSUMER
        )
        TESTHELPERS.patch_with_rabbit_mock(testconnector)

        # Run code to be tested:
        testconnector.unpublish_all_versions(
            drs_id=DRS_ID,
            data_node=DATA_NODE)

        # Check result:
        expected_rabbit_task = {
            "operation": "unpublish_all_versions",
            "message_timestamp": "anydate",
            "data_node": DATA_NODE,
            "aggregation_level":"dataset",
            "drs_id":DRS_ID,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all',
            "consumer_solr_url":SOLR_URL_CONSUMER
        }        
        received_rabbit_msg = TESTHELPERS.get_received_message_from_rabbitmock(testconnector)
        is_same = utils.is_json_same(expected_rabbit_task, received_rabbit_msg)
        self.assertTrue(is_same, utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_msg))

    #
    # Errata
    #

    def test_add_errata_id_several_ok(self):

        # Preparations: Create patched connector
        # (Patched to avoid that message be sent, and to retrieve the created message)
        testconnector = TESTHELPERS.get_connector()
        TESTHELPERS.patch_with_rabbit_mock(testconnector)

        # Run code to be tested: Add errata
        testconnector.add_errata_ids(
            drs_id=DRS_ID,
            version_number=DS_VERSION,
            errata_ids=ERRATA_SEVERAL
        )

        # Check result: Was correct errata message created?
        expected_rabbit_task = {
            "handle": DATASETHANDLE_HDL,
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":ERRATA_SEVERAL,
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":DRS_ID,
            "version_number":DS_VERSION
        }
        received_rabbit_msg = TESTHELPERS.get_received_message_from_rabbitmock(testconnector)
        is_same = utils.is_json_same(expected_rabbit_task, received_rabbit_msg)
        self.assertTrue(is_same, utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_msg))

    def test_add_errata_id_one_ok(self):

        # Preparations: Create patched connector
        # (Patched to avoid that message be sent, and to retrieve the created message)
        testconnector = TESTHELPERS.get_connector()
        TESTHELPERS.patch_with_rabbit_mock(testconnector)
        
        # Run code to be tested: Add errata
        testconnector.add_errata_ids(
            drs_id=DRS_ID,
            version_number=DS_VERSION,
            errata_ids=ERRATA
        )

        # Check result: Was correct errata message created?
        expected_rabbit_task = {
            "handle": DATASETHANDLE_HDL,
            "operation": "add_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":[ERRATA],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.add',
            "drs_id":DRS_ID,
            "version_number":DS_VERSION
        }
        received_rabbit_msg = TESTHELPERS.get_received_message_from_rabbitmock(testconnector)
        is_same = utils.is_json_same(expected_rabbit_task, received_rabbit_msg)
        self.assertTrue(is_same, utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_msg))

    def test_remove_errata_id_one_ok(self):

        # Preparations: Create patched connector
        # (Patched to avoid that message be sent, and to retrieve the created message)
        testconnector = TESTHELPERS.get_connector()
        TESTHELPERS.patch_with_rabbit_mock(testconnector)

        # Run code to be tested: Add errata
        testconnector.remove_errata_ids(
            drs_id=DRS_ID,
            version_number=DS_VERSION,
            errata_ids=ERRATA
        )

        # Check result: Was correct errata message created?
        expected_rabbit_task = {
            "handle": DATASETHANDLE_HDL,
            "operation": "remove_errata_ids",
            "message_timestamp":"anydate",
            "errata_ids":[ERRATA],
            "ROUTING_KEY": ROUTING_KEY_BASIS+'errata.remove',
            "drs_id":DRS_ID,
            "version_number":DS_VERSION
        }
        received_rabbit_msg = TESTHELPERS.get_received_message_from_rabbitmock(testconnector)
        is_same = utils.is_json_same(expected_rabbit_task, received_rabbit_msg)
        self.assertTrue(is_same, utils.compare_json_return_errormessage(expected_rabbit_task, received_rabbit_msg))


    #
    # Data Cart
    #

    def test_make_data_cart_pid(self):

        # Test variables
        content1 = {'foo':'foo', 'bar':'bar'}
        content2 = {'foo':'foo', 'bar': None}

        # Preparations: Create patched connector
        # (Patched to avoid that message be sent, and to retrieve the created message)
        testconnector = TESTHELPERS.get_connector()
        TESTHELPERS.patch_with_rabbit_mock(testconnector)

        # Run code to be tested: Create data cart PIDs
        # And retrieve the messages
        pid1 = testconnector.create_data_cart_pid(content1)
        pid2 = testconnector.create_data_cart_pid(content2)

        # Check result: Were the correct messages created?
        expected_handle_both_cases = PREFIX_WITH_HDL+"/b597a79e-1dc7-3d3f-b689-75ac5a78167f"
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
        received_rabbit_msg1 = TESTHELPERS.get_received_message_from_rabbitmock(testconnector, 0)
        received_rabbit_msg2 = TESTHELPERS.get_received_message_from_rabbitmock(testconnector, 1)
        same1 = utils.is_json_same(expected_rabbit_task1, received_rabbit_msg1)
        same2 = utils.is_json_same(expected_rabbit_task2, received_rabbit_msg2)
        self.assertTrue(same1, error_message(expected_rabbit_task1, received_rabbit_msg1))
        self.assertTrue(same2, error_message(expected_rabbit_task2, received_rabbit_msg2))
        self.assertTrue(pid1==pid2, 'Both pids are not the same.')

    #
    # Threads
    #

    def test_start_messaging_thread(self):
        LOGGER.debug('Thread test')
        testconnector = TESTHELPERS.get_connector()
        rabbitmock = TESTHELPERS.patch_with_rabbit_mock(testconnector, mock.MagicMock())
        testconnector.start_messaging_thread()
        rabbitmock.start.assert_called_with()

    def test_finish_messaging_thread(self):
        LOGGER.debug('Thread test')
        testconnector = TESTHELPERS.get_connector()
        rabbitmock = TESTHELPERS.patch_with_rabbit_mock(testconnector, mock.MagicMock())
        testconnector.finish_messaging_thread()
        rabbitmock.finish.assert_called_with()

    def test_force_finish_messaging_thread(self):
        LOGGER.debug('Thread test')
        testconnector = TESTHELPERS.get_connector()
        rabbitmock = TESTHELPERS.patch_with_rabbit_mock(testconnector, mock.MagicMock())
        testconnector.force_finish_messaging_thread()
        rabbitmock.force_finish.assert_called_with()

    def test_make_handle(self):
        testconnector = TESTHELPERS.get_connector()
        received_handle = testconnector.make_handle_from_drsid_and_versionnumber(
            drs_id=DRS_ID,
            version_number=DS_VERSION
        )
        expected_handle = DATASETHANDLE_HDL
        self.assertEquals(received_handle, expected_handle)
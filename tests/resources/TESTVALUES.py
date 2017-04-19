import mock
import copy
import esgfpid
import tests.mocks.responsemock
import tests.mocks.solrmock
import tests.mocks.rabbitmock
import tests.utils as utils
import Queue
from esgfpid.defaults import ROUTING_KEY_BASIS as ROUTING_KEY_BASIS

# Errata
ERRATA_SEVERAL = ['123456','654321']
ERRATA = '123456'

# Solr
SOLR_URL_LIBRARY = 'http://solr.foo'
SOLR_URL_CONSUMER = 'http://solr-to-be-used-by-consumer.foo'

# RabbitMQ
RABBIT_URL_TRUSTED = 'http://trusted.rabbit.foo'
RABBIT_URL_TRUSTED2 = 'http://trusted.rabbit.bar'
RABBIT_URL_TRUSTED3 = 'http://trusted.rabbit.baz'
RABBIT_URL_OPEN = 'http://open.rabbit.foo'
EXCHANGE_NAME = 'exch'
RABBIT_USER_OPEN = 'EvilRabbit'
RABBIT_USER_TRUSTED = 'HappyRabbit'
RABBIT_PASSWORD = 'carrotDreams'
TEST_RABBIT_CREDS_OPEN = dict(
    url = RABBIT_URL_OPEN,
    user = RABBIT_USER_OPEN)
TEST_RABBIT_CREDS_TRUSTED = dict(
    url = RABBIT_URL_TRUSTED,
    user = RABBIT_USER_TRUSTED,
    password = RABBIT_PASSWORD)

# Misc
PREFIX_NO_HDL = '21.14foo'
PREFIX_WITH_HDL = 'hdl:'+PREFIX_NO_HDL

# Files
SUFFIX_FILE = 'xyz123abc'
SUFFIX_FILE2 = 'xyz123def'
FILEHANDLE_HDL  = PREFIX_WITH_HDL+'/'+SUFFIX_FILE
FILEHANDLE_NO_HDL  = PREFIX_NO_HDL+'/'+SUFFIX_FILE
FILEHANDLE2_HDL = PREFIX_WITH_HDL+'/'+SUFFIX_FILE2
FILENAME = 'mytestfilename.nc'
CHECKSUM = 'testchecksumabc'
CHECKSUMTYPE = 'SHA999'
FILESIZE = 10010
FILEVERSION = '987'
PUBLISH_PATH = 'my/publish/path'

# Datasets
DRS_ID = 'my/drs/id'
THREDDS ='test/thr/path'
DATA_NODE = 'esgf-foo.dkrz.de'
SUFFIX_DS = 'afd65cd0-9296-35bc-a706-be98665c9c36'
SUFFIX_DS2 = 'ceb26955-82ea-3610-ab41-5ebfd1533f94'
DS_VERSION = 201603
DS_VERSION2 = 201703
DATASETHANDLE_HDL = PREFIX_WITH_HDL+'/'+SUFFIX_DS
DATASETHANDLE_HDL2 = PREFIX_WITH_HDL+'/'+SUFFIX_DS2


#
# Mocks
#


def get_thread_mock():
    return MockThread()

def get_thread_mock2(error=None):
    return MockThread2(error=error)

def get_thread_mock3(error=None):
    return MockThread3(error=error)

def get_thread_mock4(error=None):
    return MockThread4(error=error)

def get_connection_mock():
    connectionmock = mock.MagicMock()
    def side_effect_add_timeout(seconds, callback):
        callback()
    connectionmock.add_timeout = mock.MagicMock()
    connectionmock.add_timeout.side_effect = side_effect_add_timeout
    return connectionmock




class MockThread(object):

    def __init__(self):
        self.num_message_events = 0
        self.start_was_called = False
        self.was_gently_finished = False
        self.was_force_finished = False
        self._is_alive = False
        self.nacked = []
        self.unconfirmed = []
        self.messages = []

    # RabbitThread API:

    def start(self):
        self.start_was_called = True
        self._is_alive = True

    def join(self, seconds):
        self.join_was_called = True
        self._is_alive = False

    def add_event_publish_message(self):
        self.num_message_events += 1

    def add_event_force_finish(self):
        self.was_force_finished = True

    def add_event_gently_finish(self):
        self.was_gently_finished = True

    def is_alive(self):
        return self._is_alive

    def get_nacked_messages_as_list(self):
        return self.nacked

    def get_unconfirmed_messages_as_list_copy(self):
        return self.unconfirmed

'''
Used for testing the thread_feeder.
'''
class MockThread2(object):

    def __init__(self, error=None):
        # Used for mocking the behaviour:
        self.messages = []
        self.put_back = []
        self.undelivered_msg = []
        self.unconfirmed_tags = []
        self.exchange_name = 'foo'
        # Rabbit API, used by modules:
        self._channel = mock.MagicMock()
        if error is not None:
            self._channel.basic_publish.side_effect = error

    def get_message_from_unpublished_stack(self, seconds):
        if len(self.messages) == 0:
            raise Queue.Empty()
        else:
            return self.messages.pop()

    def get_num_unpublished(self):
        return len(self.messages)

    def get_open_word_for_routing_key(self):
        return 'foo'

    def put_one_message_into_queue_of_unsent_messages(self, msg):
        self.messages.append(msg)
        self.put_back.append(msg)

    def get_exchange_name(self):
        return self.exchange_name

    def put_to_unconfirmed_delivery_tags(self, tag):
        self.unconfirmed_tags.append(tag)

    def put_to_unconfirmed_messages_dict(self, tag, msg):
        self.undelivered_msg.append(msg)


'''
Used for testing the thread_returner.
'''
class MockThread3(object):

    def __init__(self, error=None):
        # Attributes (used by modules):
        self._channel = mock.MagicMock()
        # Methods (used by modules):
        self.send_a_message = mock.MagicMock()

        if error is not None:
            self.send_a_message.side_effect = error

'''
Used for testing the thread_returner.
'''
class MockThread4(object):

    def __init__(self, error=None):
        self.num_unpublished = 0
        self.num_unconfirmed = 0.0
        self.fake_confirms = False
        self.confirm_rate = 1
        # Attributes (used by modules):
        self.ERROR_CODE_CONNECTION_FORCE_CLOSED = 999
        self.ERROR_TEXT_CONNECTION_FORCE_CLOSED = "errortext"
        self.ERROR_CODE_CONNECTION_CLOSED_BY_USER = 888
        self.ERROR_TEXT_CONNECTION_CLOSED_BY_USER = "bla"
        self.ERROR_CODE_CONNECTION_NORMAL_SHUTDOWN = 777
        self.ERROR_TEXT_CONNECTION_NORMAL_SHUTDOWN = 'bli'
        self._connection = mock.MagicMock()
        self._connection.is_closed = False
        self._connection.is_closing = False
        self._connection.is_open = True

        # Implement callback
        def side_effect(wait_seconds, callback):
            callback()
        self._connection.add_timeout.side_effect = side_effect

        # Methods (used by modules):
        self.tell_publisher_to_stop_waiting_for_gentle_finish = mock.MagicMock()
        self.make_permanently_closed_by_user = mock.MagicMock()
        self.add_event_publish_message = mock.MagicMock()
        self.add_event_publish_message.side_effect = self.side_effect_add_event_publish_message

    def get_num_unpublished(self):
        #print('Unpublished: '+str(self.num_unpublished))
        return int(round(self.num_unpublished))

    def get_num_unconfirmed(self):
        num_conf = self.num_unconfirmed
        # This fakes receiving confirms from Rabbit:
        if self.fake_confirms:
            self.num_unconfirmed = self.num_unconfirmed - self.confirm_rate
            if (self.num_unconfirmed < 0):
                self.num_unconfirmed = 0
        # This returns num confirms
        #print('Unconfirmed: '+str(self.num_unconfirmed))
        return num_conf

    def side_effect_add_event_publish_message(self):
        self.num_unpublished = self.num_unpublished -1
        if (self.num_unpublished < 0):
            self.num_unpublished = 0
        #print('Called add_event_publish_message! Now: %i' % self.num_unpublished)

    def set_fake_confirms(self):
        self.fake_confirms = True






class MockRabbitSender(object):

    def __init__(self):
        self.received_messages = []
        self.please_print = False

    def send_message_to_queue(self, msg):
        self.received_messages.append(msg)
        if self.please_print:
            print('Called "send_message_to_queue()" with '+str(msg))

    def open_rabbit_connection(self):
        if self.please_print:
            print('Called "open_rabbit_connection()"')

    def close_rabbit_connection(self):
        if self.please_print:
            print('Called "close_rabbit_connection()"')

    def start(self):
        if self.please_print:
            print('Called "start()"')

    def finish(self):
        if self.please_print:
            print('Called "finish()"')

    def force_finish(self):
        if self.please_print:
            print('Called "force_finish()"')

#
# Provide a coupler for unit tests
# that can be mocked with solr mock, rabbit mock...
#

def get_coupler_args(**kwargs):
    cred_copy = copy.deepcopy(TEST_RABBIT_CREDS_TRUSTED)
    coupler_args = dict(
        handle_prefix = PREFIX_NO_HDL,
        messaging_service_credentials = [cred_copy],
        messaging_service_exchange_name = EXCHANGE_NAME,
        data_node = DATA_NODE,
        thredds_service_path = THREDDS,
        solr_url = SOLR_URL_LIBRARY,
        solr_https_verify=True,
        solr_switched_off=False,
        test_publication=False,
        message_service_synchronous=False
    )
    for k,v in kwargs.iteritems():
        coupler_args[k] = v
    return coupler_args

def get_coupler(**kwargs):
    coupler_args = get_coupler_args(**kwargs)
    testcoupler = esgfpid.coupling.Coupler(**coupler_args)
    return testcoupler


#
# Provide a connector
# that can be mocked with solr mock, rabbit mock...
#

def get_rabbit_credentials(**kwargs):
    rabbit_args = dict(
        user = RABBIT_USER_TRUSTED,
        url = RABBIT_URL_TRUSTED,
        password = RABBIT_PASSWORD
    )
    for k,v in kwargs.iteritems():
        rabbit_args[k] = v
    return rabbit_args

def get_connector_with_rabbit(**kwargs):
    cred = get_rabbit_credentials(**kwargs)
    return get_connector(messaging_service_credentials=[cred])

def get_connector_args(**kwargs):
    cred_copy = copy.deepcopy(TEST_RABBIT_CREDS_TRUSTED)
    connector_args = dict(
        handle_prefix = PREFIX_NO_HDL,
        messaging_service_exchange_name = EXCHANGE_NAME,
        messaging_service_credentials = [cred_copy]
    )
    for k,v in kwargs.iteritems():
        connector_args[k] = v
    return connector_args

def get_connector(**kwargs):
    connector_args = get_connector_args(**kwargs)
    return esgfpid.Connector(**connector_args)

#
# Patching RabbitMQ
#

def patch_with_rabbit_mock(connector_or_coupler, rabbitmock=None):
    if rabbitmock is None:
        rabbitmock = MockRabbitSender()
    if type(connector_or_coupler) == esgfpid.Connector:
        connector_or_coupler._Connector__coupler._Coupler__rabbit_message_sender = rabbitmock
    else:
        connector_or_coupler._Coupler__rabbit_message_sender = rabbitmock
    return rabbitmock

def get_received_message_from_rabbitmock(connector_or_coupler, index=0):
    if type(connector_or_coupler) == esgfpid.Connector:
        return _get_received_message_from_connector_rabbitmock(connector_or_coupler, index)
    else:
        return _get_received_message_from_coupler_rabbitmock(connector_or_coupler, index)

def _get_received_message_from_connector_rabbitmock(connector, index=0):
    coupler = connector._Connector__coupler
    rabbitmock = coupler._Coupler__rabbit_message_sender
    msg = rabbitmock.received_messages[index] #  if using MockRabbitSender
    #msg = rabbitmock.send_message_to_queue.call_args[0][index] # if using MagicMock (first get positional args, then get the i'th of those)
    tests.utils.replace_date_with_string(msg)
    return msg

def _get_received_message_from_coupler_rabbitmock(coupler, index=0):
    rabbitmock = coupler._Coupler__rabbit_message_sender
    msg = rabbitmock.received_messages[index] # if using MockRabbitSender
    #msg = rabbitmock.send_message_to_queue.call_args[0][index] # if using MagicMock (first get positional args, then get the i'th of those)
    tests.utils.replace_date_with_string(msg)
    return msg

#
# Patching solr
#

def patch_solr_returns_empty_file_list(connector_or_coupler):
    return patch_solr_returns_previous_files(connector_or_coupler, [])

def patch_solr_returns_previous_files(connector_or_coupler, previous_files):
    solrmock = mock.Mock()
    solrmock.retrieve_file_handles_of_same_dataset = mock.Mock()
    solrmock.retrieve_file_handles_of_same_dataset.return_value = previous_files
    patch_with_solr_mock(connector_or_coupler, solrmock)

def patch_solr_returns_list_of_datasets_and_versions(connector_or_coupler, datasets, versions):
    solrmock = mock.Mock()
    methodmock = mock.Mock()
    methodmock.return_value = dict(dataset_handles=datasets, version_numbers=versions)
    solrmock.retrieve_datasethandles_or_versionnumbers_of_allversions = methodmock
    patch_with_solr_mock(connector_or_coupler, solrmock)

def patch_solr_raises_error(connector_or_coupler, error_to_be_raised):
    solrmock = mock.Mock()
    methodmock = mock.Mock()
    methodmock.side_effect = error_to_be_raised
    solrmock.retrieve_file_handles_of_same_dataset = methodmock
    solrmock.retrieve_datasethandles_or_versionnumbers_of_allversions = methodmock
    patch_with_solr_mock(connector_or_coupler, solrmock)

def patch_with_solr_mock(connector_or_coupler, solrmock):
    solrmock.is_switched_off = mock.Mock()
    solrmock.is_switched_off.return_value = False
    if type(connector_or_coupler) == esgfpid.Connector:
        connector_or_coupler._Connector__coupler._Coupler__solr_sender = solrmock
    else:
        connector_or_coupler._Coupler__solr_sender = solrmock




#####

def get_rabbit_args(**kwargs):
    if not 'is_synchronous_mode' in kwargs:
        raise ValueError('Please specify argument "is_synchronous_mode"!')
    cred_copy = copy.deepcopy(TEST_RABBIT_CREDS_TRUSTED)
    rabbit_args = dict(
        credentials = [cred_copy],
        exchange_name='exch',
        test_publication=False,
        is_synchronous_mode=kwargs['is_synchronous_mode']
    )
    for k,v in kwargs.iteritems():
        rabbit_args[k] = v
    return rabbit_args

def get_rabbit_message_sender(**kwargs):
    args = get_rabbit_args(**kwargs)
    testrabbit = esgfpid.rabbit.RabbitMessageSender(**args)
    return testrabbit

def patch_rabbit_with_magic_mock(testrabbit):
    testrabbit._RabbitMessageSender__server_connector = mock.MagicMock()

def get_nodemanager():
    nodemanager = esgfpid.rabbit.nodemanager.NodeManager()
    nodemanager.add_trusted_node(
        username=RABBIT_USER_TRUSTED,
        host = RABBIT_URL_TRUSTED,
        exchange_name = EXCHANGE_NAME,
        password=RABBIT_PASSWORD,
        priority=1)
    nodemanager.add_trusted_node(
        username=RABBIT_USER_TRUSTED,
        host = RABBIT_URL_TRUSTED2,
        exchange_name = EXCHANGE_NAME,
        password=RABBIT_PASSWORD,
        priority=2)
    nodemanager.add_trusted_node(
        username=RABBIT_USER_TRUSTED,
        host = RABBIT_URL_TRUSTED3,
        exchange_name = EXCHANGE_NAME,
        password=RABBIT_PASSWORD,
        priority=3)
    return nodemanager

def get_synchronous_rabbit():
    return esgfpid.rabbit.synchronous.SynchronousRabbitConnector(get_nodemanager())

def get_asynchronous_rabbit():
    return esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(get_nodemanager())


#
# Solr
#

def get_testsolr(**kwargs):
    solr_args = dict(
        solr_url = SOLR_URL_LIBRARY,
        prefix = PREFIX_NO_HDL,
        switched_off = False,
        https_verify = True,
        disable_insecure_request_warning = False
    )
    for k,v in kwargs.iteritems():
        solr_args[k] = v
    testsolr = esgfpid.solr.SolrInteractor(**solr_args)
    return testsolr

def get_testsolr_switched_off():
    return get_testsolr(switched_off=True)

def get_testsolr_connector():
    testsolr = esgfpid.solr.serverconnector.SolrServerConnector(
        solr_url = SOLR_URL_LIBRARY,
        https_verify = True,
        disable_insecure_request_warning = False
    )
    return testsolr

#
# Args for common tasks...
#

def get_args_for_publication_assistant():
    # Coupler has to be added!
    return dict(
        prefix = PREFIX_NO_HDL,
        drs_id = DRS_ID,
        version_number=DS_VERSION,
        is_replica=False,
        thredds_service_path = THREDDS,
        data_node = DATA_NODE,
        consumer_solr_url = SOLR_URL_CONSUMER
    )

def get_args_for_adding_file():
    return dict(
        file_name = FILENAME,
        file_handle = PREFIX_WITH_HDL+'/'+SUFFIX_FILE,
        checksum = CHECKSUM,
        file_size = FILESIZE,
        publish_path = PUBLISH_PATH,
        prefix = PREFIX_NO_HDL,
        checksum_type = CHECKSUMTYPE,
        file_version = FILEVERSION
    )

def get_args_for_unpublish_one():
    # Coupler has to be added!
    return dict(
        drs_id=DRS_ID,
        prefix=PREFIX_NO_HDL,
        data_node=DATA_NODE)

def get_args_for_unpublish_all():
    # Coupler has to be added!
    return dict(
        drs_id=DRS_ID,
        prefix=PREFIX_NO_HDL,
        data_node=DATA_NODE)

def get_args_for_consistency_check():
    # Coupler has to be added!
    return dict(
        drs_id = DRS_ID,
        data_node = DATA_NODE,
        version_number = DS_VERSION)

def get_args_for_nodemanager(**kwargs):
    args = dict(
        exchange_name='exch_foo',
        host='host_foo',
        username='user_foo',
        password='pw_foo'
    )
    for k,v in kwargs.iteritems():
        args[k] = v
    return args

#
# Rabbit Messages
#

def get_rabbit_message_publication_dataset():
    expected_rabbit_task = {
        "handle" : DATASETHANDLE_HDL,
        "aggregation_level" : "dataset",
        "operation" : "publish",
        "message_timestamp" : "anydate",
        "drs_id" : DRS_ID,
        "version_number" : DS_VERSION,
        "files" : [FILEHANDLE_HDL],
        "is_replica" : False,
        "data_node" : DATA_NODE,
        "ROUTING_KEY" : ROUTING_KEY_BASIS+'publication.dataset.orig',
        "consumer_solr_url" : SOLR_URL_CONSUMER
    }
    return expected_rabbit_task

def get_rabbit_message_publication_file():
    expected_data_url = ('http://'+DATA_NODE+'/'+THREDDS+'/'+PUBLISH_PATH)
    expected_rabbit_task = {
        "handle": FILEHANDLE_HDL,
        "aggregation_level": "file",
        "operation": "publish",
        "message_timestamp": "anydate",
        "is_replica": False,
        "file_name": FILENAME,
        "data_url": expected_data_url,
        "data_node": DATA_NODE,
        "file_size": FILESIZE,
        "checksum": CHECKSUM,
        "checksum_type": CHECKSUMTYPE,
        "file_version": FILEVERSION,
        "parent_dataset": DATASETHANDLE_HDL,
        "ROUTING_KEY": ROUTING_KEY_BASIS+'publication.file.orig',
    }
    return expected_rabbit_task

def get_rabbit_message_unpub_one():
    expected_rabbit_task = {
        "handle": DATASETHANDLE_HDL,
        "aggregation_level": "dataset",
        "operation": "unpublish_one_version",
        "message_timestamp":"anydate",
        "data_node": DATA_NODE,
        "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.one',
        "drs_id":DRS_ID,
        "version_number":int(DS_VERSION)
    }
    return expected_rabbit_task

def get_rabbit_message_unpub_all():
    expected_rabbit_task = {
        "operation": "unpublish_all_versions",
        "aggregation_level": "dataset",
        "message_timestamp": "anydate",
        "drs_id": DRS_ID,
        "data_node": DATA_NODE,
        "ROUTING_KEY": ROUTING_KEY_BASIS+'unpublication.all',
    }
    return expected_rabbit_task

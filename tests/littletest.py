import esgfpid
import tests.mocks.solrmock
import tests.mocks.rabbitmock

TESTVALUES = dict(
    solr_url='http://solr.foo',
    prefix='21.14foo',
    drs_id1='my/drs/id',
    version_number1=201603,
    suffix1='afd65cd0-9296-35bc-a706-be98665c9c36',
    data_node='esgf-foo.dkrz.de',
    url_messaging_service='http://rabbit.foo',
    messaging_exchange='exch_name',
    exchange_name='exch', # TODO JUST ONE NAME!
    thredds_service_path='test/thr/path',
    rabbit_username='RogerRabbit',
    rabbit_password='carrotDreams',
    errata_ids=['123456','654321'],
    errata_id='123456',
    file_name1='mytestfilename.nc',
    file_suffix1='xyz123abc',
    file_suffix2='xyz123def',
    checksum1='testchecksumabc',
    file_size1=10010,
    publish_path1='my/publish/path',
    checksum_type1='SHA999',
    file_version1='987'
)


# Test variables:
#testcoupler = self.__make_patched_testcoupler(solr_off=True)
solr_off = True
testcoupler = esgfpid.coupling.Coupler(
    handle_prefix = TESTVALUES['prefix'],
    messaging_service_urls = 'rabbit_should_not_be_used',
    messaging_service_url_preferred = None,
    messaging_service_exchange_name = TESTVALUES['messaging_exchange'],
    messaging_service_username = TESTVALUES['rabbit_username'],
    messaging_service_password = TESTVALUES['rabbit_password'],
    data_node = TESTVALUES['data_node'],
    thredds_service_path = TESTVALUES['thredds_service_path'],
    solr_url = 'solr_should_not_be_used',
    solr_https_verify=True,
    solr_switched_off=solr_off
)
# Replace objects that interact with servers with mocks
# Patch rabbit
rabbitmock = tests.mocks.rabbitmock.SimpleMockRabbitSender()
testcoupler._Coupler__rabbit_message_sender = rabbitmock



# Step 1: Make assistant for dataset
datasetargs = dict(
    prefix=TESTVALUES['prefix'],
    drs_id=TESTVALUES['drs_id1'],
    version_number=TESTVALUES['version_number1'],
    is_replica=False,
    thredds_service_path=TESTVALUES['thredds_service_path'],
    data_node=TESTVALUES['data_node'],
    coupler=testcoupler,
    consumer_solr_url='does_not_matter'
)
assistant = esgfpid.assistant.publish.DatasetPublicationAssistant(**datasetargs)

# Step 2: Add files
fileargs =  dict(
    file_name = TESTVALUES['file_name1'],
    file_handle = 'hdl:'+TESTVALUES['prefix']+'/'+TESTVALUES['file_suffix1'],
    checksum = TESTVALUES['checksum1'],
    file_size = TESTVALUES['file_size1'],
    publish_path = TESTVALUES['publish_path1'],
    prefix = TESTVALUES['prefix'],
    checksum_type=TESTVALUES['checksum_type1'],
    file_version=TESTVALUES['file_version1'] # string
)
#assistant.add_file(**fileargs)

# Step 3: Finish publication
assistant.dataset_publication_finished()
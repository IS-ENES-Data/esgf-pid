TESTVALUES = dict(
    solr_url='http://solr.foo',
    consumer_solr_url='http://solr-to-be-used-by-consumer.foo',
    prefix='21.14foo',
    drs_id1='my/drs/id',
    version_number1=201603,
    suffix1='afd65cd0-9296-35bc-a706-be98665c9c36',
    data_node='esgf-foo.dkrz.de',
    rabbit_url_trusted='http://trusted.rabbit.foo',
    rabbit_url_open='http://open.rabbit.foo',
    rabbit_exchange_name='exch',
    thredds_service_path='test/thr/path',
    rabbit_username_open='EvilRabbit',
    rabbit_username_trusted='HappyRabbit',
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

TEST_RABBIT_CREDS_OPEN = dict(
    url = TESTVALUES['rabbit_url_open'],
    user = TESTVALUES['rabbit_username_open']
)

TEST_RABBIT_CREDS_TRUSTED = dict(
    url = TESTVALUES['rabbit_url_trusted'],
    user = TESTVALUES['rabbit_username_trusted'],
    password = TESTVALUES['rabbit_password']
)


'''
In many tests, we need a connector object.

To prevent having to adapt all unit tests whenever the 
connector API changes, we define a set of minimum connector
args here.
'''
CONNECTOR_ARGS_MIN = dict(
    handle_prefix = TESTVALUES['prefix'],
    messaging_service_exchange_name = TESTVALUES['rabbit_exchange_name'],
    messaging_service_credentials = [TEST_RABBIT_CREDS_TRUSTED]
)

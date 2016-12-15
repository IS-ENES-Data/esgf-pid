import esgfpid
import logging
import datetime

'''
A script to test all tasks of the esgfpid library.

This only tests the most basic standard cases.

Please checkout the logs to verify the correct
execution of all functionality. As the module
has two threads and some parallel, asynchronous
execution, errors tend not to show in this script.

'''

# Please fill in these values:
PREFIX = '21.14100' # CMIP6
RABBIT_URL = 'foo.rabbit.bar'
RABBIT_EXCHANGE = 'foo-exchangename'
RABBIT_USER = 'johndoe'
RABBIT_PW = 'insertme'
LOGLEVEL = logging.DEBUG
LOGLEVEL = logging.INFO
RABBIT_USER_OPEN = 'foo-user'
RABBIT_URL_OPEN = 'foo.open.bar'

# For publication:
DATA_NODE1 = 'foo.bar.test'
DATA_NODE2 = 'foo.hello.test'
THREDDS_PATH = '/foo/bar/baz/'

# 
TEST_DRS = 'test.drs.id1'
TEST_VERSION1 = 20001111
TEST_VERSION2 = 20201111

IS_TEST = False

def init_logging():
    path = 'logs'
    logdate = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    filename = path+'/test_log_'+logdate+'_'+logging.getLevelName(LOGLEVEL)+'.log'
    esgfpid.utils.ensure_directory_exists(path)
    myformat = '%(asctime)-15s %(levelname)-5s: %(name)s [%(threadName)s]: %(message)s'
    logging.basicConfig(level=LOGLEVEL, filename=filename, filemode='w', format=myformat)
    pikalogger = logging.getLogger('pika')
    pikalogger.setLevel(logging.WARNING)
    print 'Logging to file ".%s"' % filename

def init_connector(data_node):
    trusted_node1 = {
        'user':RABBIT_USER,
        'password':RABBIT_PW,
        'url':RABBIT_URL,
        'priority':1
    }
    trusted_node2 = {
        'user':RABBIT_USER,
        'password':RABBIT_PW,
        'url':RABBIT_URL,
        'priority':2
    }
    open_node = {
        'user':RABBIT_USER_OPEN,
        'url':RABBIT_URL_OPEN
    }
    connector = esgfpid.Connector(
        handle_prefix=PREFIX,
        messaging_service_credentials=[trusted_node1,trusted_node2,open_node],
        messaging_service_exchange_name=RABBIT_EXCHANGE,
        data_node=data_node,
        thredds_service_path=THREDDS_PATH,
        test_publication=IS_TEST
    )
    connector.start_messaging_thread()
    return connector

def publish_dataset(connector, version_number):
    wizard = connector.create_publication_assistant(
        drs_id=TEST_DRS,
        version_number=version_number,
        is_replica=False    
    )
    ds_handle = wizard.get_dataset_handle()
    file_handle1 = PREFIX+'/sdkfjskfjsidlfj'
    file_handle2 = PREFIX+'/jglsrirlsjjlekdfsl'
    wizard.add_file(
        file_name='temperature.nc',
        file_handle=file_handle1,
        file_size=1000,
        checksum='abc123check',
        publish_path='my/path/file.nc',
        checksum_type='SHA256',
        file_version='f1'
    )
    wizard.add_file(
        file_name='temperature.nc',
        file_handle=file_handle2,
        file_size=1000,
        checksum='abc123check',
        publish_path='my/path/differentfile.nc',
        checksum_type='SHA256',
        file_version='f1'
    )
    wizard.dataset_publication_finished()

    print('Publication done:')
    print('hdl.handle.net/%s?noredirect' % ds_handle)
    print('hdl.handle.net/%s?noredirect' % file_handle1)
    print('hdl.handle.net/%s?noredirect' % file_handle2)

def add_and_remove_errata(connector, drs_id, version_number):
    connector.add_errata_ids(
        drs_id=drs_id,
        version_number=version_number,
        errata_ids=['abc','def','ghi']
    )
    connector.remove_errata_ids(
        drs_id=drs_id,
        version_number=version_number,
        errata_ids=['def']
    )

def shopping_cart(connector):
    pid = connector.create_shopping_cart_pid({'foo':'hdl:123/345','bar':None,'baz':'hdl:123/678'})
    print('Shopping cart:')
    print('hdl.handle.net/%s?noredirect' % pid)

def unpublish_one_version(connector, drs_id, version_number):
    connector.unpublish_one_version(
        drs_id=drs_id,
        version_number=version_number
    )

def unpublish_all_versions(connector, drs_id):
    connector.unpublish_all_versions(
        drs_id=drs_id
    )

def cleanup(connector):
    print('Waiting for pending messages and stopping thread...')
    connector.finish_messaging_thread()
    print('Script finished.')
    print('(It may take some minutes until these PIDs are actually created).')


# Now run:
init_logging()

# Publish and unpublish at one data node:
connector1 = init_connector(DATA_NODE1)
publish_dataset(connector1, TEST_VERSION1)
unpublish_all_versions(connector1, TEST_DRS)

# Publish and unpublish at another data node:
connector2 = init_connector(DATA_NODE2)
import time
time.sleep(3)
publish_dataset(connector2, TEST_VERSION1)
unpublish_one_version(connector2, TEST_DRS, TEST_VERSION1)
publish_dataset(connector2, TEST_VERSION2)
add_and_remove_errata(connector2, TEST_DRS, TEST_VERSION1)

# Shopping cart
shopping_cart(connector2)

# Cleanup
cleanup(connector1)
cleanup(connector2)

print('Done!')
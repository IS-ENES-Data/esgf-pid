import esgfpid
import logging
import sys
import datetime

input('Make sure you have an exchange "test123" ready, including a queue and the required bindings (see inside this script). Ok? (press any key)')

if not len(sys.argv) == 4:
    print('Please call with <host> <user> <password>!')
    exit(999)

# Please fill in functioning values!
HOST = sys.argv[1]
USER = sys.argv[2]
PW = sys.argv[3]
VHOST = 'esgf-pid'
AMQP_PORT = 5672
SSL = False
EXCH = 'test123'
# THis exchange needs to have bindings to a queue using these routing keys:
# 2114100.HASH.fresh.publi-ds-repli
# 2114100.HASH.fresh.publi-file-repli
# 2114100.HASH.fresh.unpubli-allvers
# 2114100.HASH.fresh.unpubli-onevers
# PREFIX.HASH.fresh.preflightcheck             <----- mandatory!
# UNROUTABLE.UNROUTABLE.fresh.UNROUTABLE
# 2114100.HASH.fresh.datacart
# 2114100.HASH.fresh.errata-add
# 2114100.HASH.fresh.errata-rem

# Dummy values
pid_prefix = '21.14100'
pid_data_node = 'our.data.node.test'
thredds_service_path = '/my/thredds/path/'
test_publication = True
datasetName = 'myDatasetName'
versionNumber = '20209999'
is_replica = True
file_name = 'myFunnyFile.nc'
trackingID = '%s/123456789999' % pid_prefix
checksum = 'checki'
file_size = '999'
publishPath = '/my/publish/path'
checksumType = 'MB99'
fileVersion = '2020999'


# Configure logging
root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)
pikalogger = logging.getLogger('pika')
pikalogger.setLevel(logging.INFO)
# File for error and warn
filename = './log_preflight_check_method.log'
handler = logging.FileHandler(filename=filename)
handler.setFormatter(formatter)
handler.setLevel(logging.WARN)
root.addHandler(handler)
LOGGER = logging.getLogger(__name__)
LOGGER.warning('________________________________________________________')
LOGGER.warning('___________ STARTING SCRIPT: CHECK METHOD! ______________')
LOGGER.warning('___________ %s ___________________________' % datetime.datetime.now().strftime('%Y-%m-%d_%H_%M'))



# Create credentials
# (This does not connect)
creds = dict(
    url=HOST,
    user=USER,
    password=PW,
    vhost=VHOST,
    port=AMQP_PORT,
    ssl_enabled=SSL)

# Create a connector
# (This does not connect)
pid_connector = esgfpid.Connector(
    handle_prefix=pid_prefix,
    messaging_service_exchange_name=EXCH,
    messaging_service_credentials=[creds], # list of dicts
    data_node=pid_data_node,
    thredds_service_path=thredds_service_path,
    test_publication=test_publication)


# Check the connection:



# Without sending
#check_pid_connection(pid_connector, send_message=False) # does not exist in esgfpid
x = pid_connector.check_pid_queue_availability(send_message=False)
print(x)
if x is None:
    print('1/2: Success, I think')
else:
    print('1/2: Fail, I think')


# With sending
#check_pid_connection(pid_connector, send_message=True) # does not exist in esgfpid
LOGGER.warning('___________ NOW WITH MESSAGE____________________________')
y = pid_connector.check_pid_queue_availability(send_message=True)
print(y)
if y is None:
    print('2/2: Success, I think')
else:
    print('2/2: Fail, I think')



print('Check log for errors (none expected)')
tmp = 'Routing Key:\t"PREFIX.HASH.fresh.preflightcheck"\nContent:\t"PLEASE PRINT: Testing pre-flight check..."'
print('Check queue for one new message:\n%s' % tmp)
LOGGER.warning('___________ DONE _______________________________________')




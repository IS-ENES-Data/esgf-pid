import esgfpid
import logging
import sys
import datetime


print('Make sure you have an exchange "test123" ready, including a queue and the required bindings (see inside this script).')
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
# This exchange needs to have bindings to a queue using these routing keys:
# 2114100.HASH.fresh.unpubli-allvers
# 2114100.HASH.fresh.unpubli-onevers
# 2114100.HASH.fresh.publi-ds-repli
# 2114100.HASH.fresh.publi-file-repli
# PREFIX.HASH.fresh.preflightcheck
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
filename = './log_wrong_exchange.log'
handler = logging.FileHandler(filename=filename)
handler.setFormatter(formatter)
handler.setLevel(logging.WARN)
root.addHandler(handler)
LOGGER = logging.getLogger(__name__)
LOGGER.warning('________________________________________________________')
LOGGER.warning('___________ STARTING SCRIPT: WRONG EXCHANGE! ___________')
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
    messaging_service_exchange_name=EXCH+'blablabla',
    messaging_service_credentials=[creds], # list of dicts
    data_node=pid_data_node,
    thredds_service_path=thredds_service_path,
    test_publication=test_publication)

# File and dataset publication:
pid = pid_connector.make_handle_from_drsid_and_versionnumber(
    drs_id=datasetName,
    version_number=versionNumber)

pid_wizard = pid_connector.create_publication_assistant(
    drs_id=datasetName,
    version_number=versionNumber,
    is_replica=is_replica)

pid_wizard.add_file(
    file_name=file_name,
    file_handle=trackingID,
    checksum=checksum,
    file_size=file_size,
    publish_path=publishPath,
    checksum_type=checksumType,
    file_version=fileVersion)


# Open the connection, send messages, close
pid_connector.start_messaging_thread()
pid_wizard.dataset_publication_finished()
pid_connector.finish_messaging_thread()

print('Check log for errors! We expect a lot: "Received remote Channel.Close (404): "NOT_FOUND" and "At close down: 2 pending messages (2 unpublished"')
print('Check queue, should have NO new message!')
LOGGER.warning('___________ DONE _______________________________________')


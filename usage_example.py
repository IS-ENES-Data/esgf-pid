
# Setup logging to console
import logging
import sys
root = logging.getLogger()
root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler )

# RabbitMQ connection config:
# Please fill in:
HOST = 'my-rabbit.ra'
USER = 'foobar'
PW = 'bazfoo'
SSL = False
VHOST = 'foobar'
EXCH = 'foobar'
AMQP_PORT = 5672
HTTP_PORT = 15672 # Default RabbitMQ http port, if configured
PREFIX = '21.14100'
TEST = True # only change if you know what you're doing!!!


# Create connector
import esgfpid
creds = dict(url=HOST, user=USER, password=PW, vhost=VHOST, port=AMQP_PORT, ssl_enabled=SSL)
args = dict(messaging_service_credentials=[creds], messaging_service_exchange_name=EXCH, handle_prefix=PREFIX, test_publication=TEST)        
con = esgfpid.Connector(**args)

# Check queue availability:
x = con.check_pid_queue_availability()
print(x)

# Send 
con.start_messaging_thread()
errata_args = dict(errata_ids=["aaa", "bbb"], drs_id="fooz", version_number=11110531)
con.add_errata_ids(**errata_args)
con.finish_messaging_thread()


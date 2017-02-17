

# List of prefixes
ACCEPTED_PREFIXES = ['21.14100','21.14foo']


# Custom logging
LOG_INFO_TO_DEBUG=False # info messages are printed as "debug" to not appear on publisher console. Should be False, unless you want to almost silence it.
LOG_DEBUG_TO_INFO=False # debug messages are printed as "info" to appear on publisher console. Should be False, except for during tests.
LOG_TRACE_TO_DEBUG=False # Print extremely detailed log messages of rabbit module as "DEBUG" messages. Should be False, except for during tests.
LOG_SHOW_TO_INFO=False # So I can selectively show some log messages to Katharina without having to modify the whole code. Deprecated.

# Solr:
SOLR_HTTPS_VERIFY_DEFAULT=False
SOLR_QUERY_DISTRIB=False

# Rabbit
ROUTING_KEY_BASIS = 'cmip6.publisher.HASH.'
RABBIT_DEFAULT_ROUTING_KEY=ROUTING_KEY_BASIS+'fallback' # Default, if none is included in message
RABBIT_EMERGENCY_ROUTING_KEY='UNROUTABLE' # If the message was returned as unroutable by the sender
_persistent = 2
_nonpersistent = 1
RABBIT_DELIVERY_MODE = _persistent # 'delivery_mode': See https://pika.readthedocs.org/en/0.9.6/examples/comparing_publishing_sync_async.html#comparing-message-publishing-with-blockingconnection-and-selectconnection
RABBIT_MANDATORY_DELIVERY = True  # 'mandatory':  "This flag tells the server how to react if the message cannot be routed to a queue. If this flag is set, the server will return an unroutable message with a Return method. If this flag is zero, the server silently drops the message." # See: http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish
RABBIT_FALLBACK_EXCHANGE_NAME = "FALLBACK"


# Reconnection
'''
If connection to all RabbitMQ hosts failed, how many seconds
should we wait until we start over retrying to connect
to all hosts, one after the other?
'''
RABBIT_RECONNECTION_SECONDS=0.5

'''
How many times should we retry connecting to all hosts, after
all hosts have failed?
It's preferable to try few times and throw an exception. If
all hosts fail, the problem is not likely to be solved by
waiting a few more times.
'''
RABBIT_RECONNECTION_MAX_TRIES=2


# Gentle close-down
'''
Gentle finish of the thread:
How many times should the algorithm recheck whether messages
or confirmations are still pending?
'''
RABBIT_FINISH_MAX_TRIES=10

'''
Gentle finish of the thread:
How many seconds should the algorithm wait before rechecking
whether messages or confirmations are still pending?

'''
RABBIT_FINISH_WAIT_SECONDS=0.5

# pika.ConnectionParameters

'''
Needed for pika.ConnectionParameters
Set in esgfpid.rabbit.nodemanager
'''
RABBIT_SOCKET_TIMEOUT=2 # defaults to 0.25 sec

'''
Needed for pika.ConnectionParameters
Set in esgfpid.rabbit.nodemanager
'''
RABBIT_CONNECTION_ATTEMPTS=1 # defaults to 1

'''
Needed for pika.ConnectionParameters
Set in esgfpid.rabbit.nodemanager
'''
RABBIT_CONNECTION_RETRY_DELAY_SECONDS=1 # no default




# Other
RABBIT_LOG_MESSAGE_INCREMENT = 10

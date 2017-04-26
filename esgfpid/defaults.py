# Default value for the esgfpid library

# List of prefixes
ACCEPTED_PREFIXES = ['21.14100','21.14foo']


# Custom logging
LOG_INFO_TO_DEBUG=False # info messages are printed as "debug" to not appear on publisher console. Should be False, unless you want to almost silence it.
LOG_DEBUG_TO_INFO=False # debug messages are printed as "info" to appear on publisher console. Should be False, except for during tests.
LOG_TRACE_TO_DEBUG=False # Print extremely detailed log messages of rabbit module as "DEBUG" messages. Should be False, except for during tests.
LOG_SHOW_TO_INFO=False # So I can selectively show some log messages to Katharina without having to modify the whole code. Deprecated.
RABBIT_LOG_MESSAGE_INCREMENT = 10

# Solr:
SOLR_HTTPS_VERIFY_DEFAULT=False
SOLR_QUERY_DISTRIB=False

# Rabbit
RABBIT_IS_ASYNCHRONOUS = True
ROUTING_KEY_BASIS = 'cmip6.publisher.HASH.'
RABBIT_DEFAULT_ROUTING_KEY=ROUTING_KEY_BASIS+'fallback' # Default, if none is included in message
RABBIT_EMERGENCY_ROUTING_KEY='UNROUTABLE' # If the message was returned as unroutable by the sender
_persistent = 2
_nonpersistent = 1
RABBIT_DELIVERY_MODE = _persistent # 'delivery_mode': See https://pika.readthedocs.org/en/0.9.6/examples/comparing_publishing_sync_async.html#comparing-message-publishing-with-blockingconnection-and-selectconnection
RABBIT_MANDATORY_DELIVERY = True  # 'mandatory':  "This flag tells the server how to react if the message cannot be routed to a queue. If this flag is set, the server will return an unroutable message with a Return method. If this flag is zero, the server silently drops the message." # See: http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish
RABBIT_FALLBACK_EXCHANGE_NAME = "FALLBACK"

# Rabbit module, values for pika
# See http://pika.readthedocs.io/en/latest/_modules/pika/connection.html
RABBIT_PIKA_SOCKET_TIMEOUT=0.25 # defaults to 0.25 sec
RABBIT_PIKA_CONNECTION_ATTEMPTS=1 # defaults to 1
RABBIT_PIKA_CONNECTION_RETRY_DELAY_SECONDS=0 # defaults to 2.0
# Rabbit reconnection attempts
RABBIT_RECONNECTION_SECONDS=0.5 # after how much time try to retry connecting to same hosts if connection was closed?
RABBIT_RECONNECTION_MAX_TRIES=2 # how many times should the module try reconnecting? Not so many times, rather throw exception to publisher.
# Rabbit attempts to send message (synchronous only):
RABBIT_SYN_MESSAGE_MAX_TRIES=3
RABBIT_SYN_MESSAGE_TIMEOUT_MILLISEC=10
# Rabbit closing down algorithm (asynchronous only):
RABBIT_ASYN_FINISH_MAX_TRIES=10 # How many times to recheck if all messages are published+confirmed (on finish)
RABBIT_ASYN_FINISH_WAIT_SECONDS=0.5 # How much time to wait until you recheck (on finish)


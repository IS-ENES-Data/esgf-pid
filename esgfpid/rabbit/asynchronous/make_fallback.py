import esgfpid.defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times

def define_fallback_exchange(CHANNEL):
    exchange_name = esgfpid.defaults.RABBIT_FALLBACK_EXCHANGE_NAME
    queue_name = esgfpid.defaults.RABBIT_FALLBACK_EXCHANGE_NAME
    routing_key = esgfpid.defaults.ROUTING_KEY_BASIS
    routing_key = routing_key.replace('HASH', '*')
    routing_key = routing_key+'.#'
    routing_key = routing_key.replace('..', '.')

    # Declare exchange
    try:
        loginfo(LOGGER, 'Declaration of fallback exchange "%s"...' % exchange_name)
        CHANNEL.exchange_declare(exchange_name, passive=False, durable=True, exchange_type='topic')
        loginfo(LOGGER, 'Declaration of fallback exchange "%s"... done.' % exchange_name)
    except (pika.exceptions.ChannelClosed) as e:
        loginfo(LOGGER, 'Declaration of fallback exchange "%s"... failed. Reasons: %s' % (exchange_name,e))
        #self.channel = self.__open_channel(self.connection)

    # Declare queue
    try:
        loginfo(LOGGER, 'Declaration of fallback queue "%s"...' % queue_name)
        CHANNEL.queue_declare(queue_name, passive=False, durable=True)
        loginfo(LOGGER, 'Declaration of fallback queue "%s"... done.' % queue_name)
    except (pika.exceptions.ChannelClosed) as e:
        loginfo(LOGGER, 'Declaration of fallback queue "%s"... failed. Reasons: %s' % (queue_name,e))
        #self.channel = self.__open_channel(self.connection)

    # Bind routing key
    try:
        loginfo(LOGGER, 'Binding of fallback queue with routing key "%s"...' % routing_key)
        CHANNEL.queue_bind(
            queue = queue_name,
            exchange = exchange_name,
            routing_key = routing_key
        )
        loginfo(LOGGER, 'Binding of fallback queue with routing key "%s"... done.' % routing_key)
    except (pika.exceptions.ChannelClosed) as e:
        loginfo(LOGGER, 'Binding of fallback queue with routing key "%s"... failed. Reasons: %s' % (queue_name,e))
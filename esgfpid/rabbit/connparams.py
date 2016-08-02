import esgfpid.defaults
import pika

def get_connection_parameters(credentials, host): # TODO: THis is static. Store as attribute?
    '''
    https://pika.readthedocs.org/en/0.9.6/connecting.html
    class pika.connection.ConnectionParameters(
        host=None, port=None, virtual_host=None, credentials=None, channel_max=None,
        frame_max=None, heartbeat_interval=None, ssl=None, ssl_options=None,
        connection_attempts=None, retry_delay=None, socket_timeout=None, locale=None,
        backpressure_detection=None)
    '''

    # Defaults:
    socket_timeout = esgfpid.defaults.RABBIT_ASYN_SOCKET_TIMEOUT
    connection_attempts = esgfpid.defaults.RABBIT_ASYN_CONNECTION_ATTEMPTS
    retry_delay = esgfpid.defaults.RABBIT_ASYN_CONNECTION_RETRY_DELAY_SECONDS

    # https://pika.readthedocs.org/en/0.9.6/connecting.html
    return pika.ConnectionParameters(
        host=host, # TODO: PORTS ETC.
        credentials=credentials,
        socket_timeout=socket_timeout,
        connection_attempts=connection_attempts,
        retry_delay=retry_delay
    )


def get_credentials(username, password):
    return pika.PlainCredentials(
        username,
        password
    )

def get_properties_for_message_publications():
    properties = pika.BasicProperties(
        delivery_mode=esgfpid.defaults.RABBIT_DELIVERY_MODE,
        content_type='application/json',
    )
    return properties
import unittest
import argparse
import logging
import datetime
import uuid
import pika.exceptions
import esgfpid
import esgfpid.utils
from esgfpid.exceptions import MessageNotDeliveredException
from esgfpid.rabbit.exceptions import PIDServerException

# Setup logging:
path = 'logs'
logdate = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
filename = path+'/rabbitmqtest_log_'+logdate+'.log'
esgfpid.utils.ensure_directory_exists(path)
logging.basicConfig(level=logging.DEBUG, filename=filename, filemode='w')
pikalogger = logging.getLogger('pika')
pikalogger.setLevel(logging.INFO)

# Example calls

# Simple call:
#rabbitmqtest -ho foo.com -u user -p passy

# Simple call with a virtual host
#rabbitmqtest -ho foo.com -u user -p passy -vh foo

# Simple call with pika logs to console:
#rabbitmqtest -ho foo.com -u user -p passy -pik
# (This argument can always be added)

# Message test:
#rabbitmqtest -ho foo.com -u user -p passy -m foomessage -ex exch
# (If message given, exchange needs to be given!)

# Production test: Sends a message to production queue 
#rabbitmqtest -ho foo.com -u user -p passy -prod

# -pik (print pika log messages) can always be given
# -px (prefix) can always be passed
# -m (message) needs -ex (exchange). The true exchanges may not be used for this.
# -prod (production test) ignores -m and -exch


# Functions
def close_connection():
    print('Closing connection...')
    if synchronous_mode:
        coupler.done_with_rabbit_business()
    else:
        coupler.finish_rabbit_connection()
    print('Closing connection... done.')

def exit_program():
    print('Exiting...')
    exit()

def send_a_message():
    success = False
    print('\nTesting to send test message to host %s (exchange %s)...' % (host, exch))
    print('Test message: %s' % message)
    try:
        print('Sending message...')
        coupler.send_message_to_queue(message)
        print('Sending message... done.')
        success = True
    except MessageNotDeliveredException as e:
        error_name = e.__class__.__name__
        error_message = str(e)
        print('Sending message... failed. Reason: %s (%s)' % (error_message,error_name))
    except pika.exceptions.ChannelClosed as e:
        error_name = e.__class__.__name__
        error_message = str(e)
        if ('404, "NOT_FOUND - no exchange \'%s\' in vhost' % exch) in str(e):
            print('Sending message... failed. Reason: Wrong exchange name "%s".' % exch)
            print('Exception: %s %s' % (error_name, error_message))
        else:
            print('Sending message... failed. Reason: Channel closed.')
            print('Exception: %s %s' % (error_name, error_message))
    return success


if __name__ == '__main__':
    desc = ('Test the connection to your RabbitMQ host via the esgfpid library.')
    
    # Argument parsing
    parser = argparse.ArgumentParser(description=desc)
    
    # Mandatory, for connection
    parser.add_argument('-ho','--host', nargs=1, action='store', required=True,
                   help='The RabbitMQ host to connect to.')
    parser.add_argument('-u','--user', nargs=1, action='store', required=True,
                   help='The RabbitMQ user name.')
    parser.add_argument('-p','--password', nargs=1, action='store', required=True,
                   help='The RabbitMQ password. Please escape "!" and "$" and "`" with backslashes!')
    # Optional, for connection
    parser.add_argument('-vh', '--virtualhost', nargs='?', action='store',
                    help='The virtual host to connect to.')
    parser.add_argument('-po', '--port', nargs='?', action='store',
                    help='The port to connect to.')
    # Optional, for sending messages:
    parser.add_argument('-m', '--message', nargs='?', action='store',
                    help='The test message to send. Can be any string.')
    parser.add_argument('-ex','--exchange_name', nargs='?', action='store',
                    help='The exchange name to send the message to.')
    parser.add_argument('-px', '--prefix', nargs='?', action='store',
                    help='The handle prefix to use.')
    # Other:
    parser.add_argument('-asy', '--asynchron', action='store_true',
                    help='The test message to send.')
    parser.add_argument('-pik', '--pikalog', action='store_true',
                    help='The test message to send.')
    parser.add_argument('-prod', '--production-test', action='store_true',
                    help='The test message to send.')

    # Parse the args:
    param = parser.parse_args()

    # Get mandatory args
    user = param.user[0]
    password = param.password[0]
    if '\!' in password:
        print('Replacing "\!" in password with "!".')
        password = password.replace('\!', '!')
    host = param.host[0]

    # Get optional args
    message = None
    if param.message is not None:
        message = param.message
    prefix = None
    if param.prefix is not None:
        prefix = param.prefix[0]
    exch = None
    if param.exchange_name is not None:
        exch = param.exchange_name
    prod_test = False
    if param.production_test is not None and param.production_test == True:
        prod_test = True
    synchronous_mode = True
    if param.asynchron is not None and param.asynchron == True:
        synchronous_mode = False
    vhost = None
    if param.virtualhost is not None:
        vhost = param.virtualhost
    port = None
    if param.port is not None:
        port = param.port

    # Switch on pika log:
    if param.pikalog is not None and param.pikalog == True:
        han = logging.StreamHandler()
        formatter = logging.Formatter('\tpika log: %(asctime)s - %(name)s - %(levelname)s - %(message)s')
        han.setFormatter(formatter)
        han.setLevel(logging.INFO)
        pikalogger.addHandler(han)

    # Using CMIP6 prefix as default:
    if prefix is None:
        prefix = esgfpid.defaults.ACCEPTED_PREFIXES[0]

    # Using exchange "foo" as default. If no message is sent, the exchange name does not matter:
    default_exchange = 'foo'
    if exch is None:
        exch = default_exchange

    # Avoid sending message to production exchange (which would confuse the consumer):
    exch_production = 'esgffed-exchange'
    exch_forbidden = [exch_production, 'esgffed-exchange-alternate']
    if exch in exch_forbidden and not prod_test:
        print('Sending a custom message to the exchange "%s" is not allowed.' % exch)
        exit_program()
    
    # Synchronous or asynchronous?
    if not synchronous_mode:
        print('Connecting in asychronous mode...')

    # Should a test message be sent after connection?
    send_message = False
    if message is not None:
        send_message = True

    # Should a test message be sent to production exchange?
    if prod_test:
        suffix = "librarytest"
        handle = '%s/%s' % (prefix, suffix)
        code = str(uuid.uuid1())
        newmessage = '{"operation":"rabbitmqtest", "handle":"%s", "code":"%s"}' % (handle, code)
        print('\nWill test the production exchange (%s).' % exch_production)
        if message is not None:
            print('Ignoring custom message %s, using %s instead.' % (message, newmessage))
        if not exch == default_exchange and not exch == exch_production:
            print('Ignoring exchange %s, using %s instead.' % (exch, exch_production))

        exch = exch_production
        message = newmessage
        send_message = True

   # Make connector
    print('\nTesting connection to RabbitMQ at %s (user %s, password ****)' % (host, user))
    cred = dict(user=user, password=password, url=host)
    if vhost is not None:
        cred['vhost'] = vhost
    if port is not None:
        cred['port'] = port
    connector = esgfpid.Connector(
        handle_prefix=prefix,
        messaging_service_exchange_name=exch,
        messaging_service_credentials=[cred],
        message_service_synchronous=synchronous_mode
    )

    #
    # Test Part 1
    #
    test1_passed = False

    # Connect
    failed = False
    coupler = connector._Connector__coupler
    if synchronous_mode:
        try:
            print('Connecting...')
            coupler.start_rabbit_business()
            test1_passed = True
            print('Connecting... done.')
        except PIDServerException as e:
            error_name = e.__class__.__name__
            error_message = str(e)
            print('Connecting... failed. Reason: %s (%s)' % (error_message,error_name))
            failed = True
    else:
        coupler.start_rabbit_connection()
        print('Parallel thread started...')


    # Did the connection succeed? (Otherwise, no test message)
    if send_message:
        if failed:
            print('\nWill not send test message. Reason: Connection failed.')
            send_message = False
            exit_program()
        elif param.exchange_name is None:
            print('\nWill not send test message. Reason: No exchange name specified. For sending a message, you need to specify an exchange to send to!')
            send_message = False
            close_connection()
            exit_program()

    #
    # Test Part 2
    # Sending a message
    #
    test2_passed = False

    if send_message:
        test2_passed = send_a_message()
    else:
        test2_passed = True

    if send_message and prod_test and test2_passed:
        pass # not implemented
        #print('\nIf the message succeeds, you should find the uuid "%s" somewhere in the record of the handle "%s".' % (code, handle))
        #print('Please checkout:\nhttp://hdl.handle.net/%s' % handle)        
    
    # At the end, close connection
    close_connection()


    # Print final message
    if test1_passed and test2_passed:
        print('\nRabbitMQ connection test done. SUCCESS!')
    else:
        print('\nRabbitMQ connection test done. FAILURE!')

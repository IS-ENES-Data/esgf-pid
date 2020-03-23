import pika
import logging
import socket
from esgfpid.utils import check_presence_of_mandatory_args
from esgfpid.utils import add_missing_optional_args_with_value_none
import esgfpid.defaults
import esgfpid.rabbit.rabbitutils
import esgfpid.utils as utils
import esgfpid.exceptions

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


'''
Check whether the RabbitMQ instances of a connector
object are available and reachable.


Example result:
*************************************************************************************
*** PROBLEM IN SETTING UP                                                         ***
*** RABBIT MESSAGING QUEUE (PID MODULE)                                           ***
*** CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:                    ***
***  - host "myrabbit.de": Connection failure (wrong host or port?).              ***
***  - host "myrabbit.de": Virtual host "foobarrr" does not exist.                ***
***  - host "myrabbit.de": Authentication failure (user foobar, password foobar). ***
***  - host "myrabbit.de": Connection failure (wrong host or port?).              ***
*** PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE.                  ***
*************************************************************************************


:param connector: Connector object, readily configured with RabbitMQ
    instance(s).
:param print_to_console: Optional. Boolean. If True, human-readable messages
    about the failed actions will be printed to screen (actual print method.)
:param print_success_to_console: Optional. Boolean. If True, human-readable
    messages about the successful actions will be printed to screen (actual
    print method.)
:param send_message: Optional. Boolean. If True, a test message will be sent to
    a RabbitMQ instance after successful connection.
:return: String message, or None. If the check passed, returns None. Otherwise,
    returns a detailed, printable string problem message - see example above.
'''
def check_pid_queue_availability(**args):
    rabbit_checker = RabbitChecker(**args)
    return rabbit_checker.check_and_inform()

class RabbitChecker(object):

    #
    # Init
    #

    def __init__(self, **args):
        mandatory_args = ['connector']
        optional_args = ['send_message', 'print_to_console', 'print_success_to_console']
        check_presence_of_mandatory_args(args, mandatory_args)
        add_missing_optional_args_with_value_none(args, optional_args)
        self.__define_all_attributes()
        self.__fill_all_attributes(args)
        self.connection = None
        self.channel = None
        self.channel_ok = False

    def __define_all_attributes(self):
        self.__print_errors_to_console = False
        self.__print_success_to_console = False
        self.__default_log_level = logging.DEBUG
        self.__error_messages = []
        self.__nodemanager = None
        self.__current_rabbit_host = None
        self.__exchange_name = None
        self.__send_message = False
        self.__prefix = None

    def __fill_all_attributes(self, args):
        self.__nodemanager = args['connector']._Connector__coupler._Coupler__rabbit_message_sender._RabbitMessageSender__node_manager
        if args['print_to_console'] is not None and args['print_to_console'] == True:
            self.__print_errors_to_console = True

        if args['print_success_to_console'] is not None and args['print_success_to_console'] == True:
            self.__print_success_to_console = True
        if args['send_message'] is not None and args['send_message'] == True:
            self.__send_message = True
        self.__prefix = args['connector'].prefix



    #
    # Perform the checks
    #

    '''
    Check whether the RabbitMQ instances that were passed to the connector
    are still available and reachable.
    All the necessary args have to be passed on init of the RabbitChecker
    object.
    '''
    def check_and_inform(self):
        self.__loginfo('Checking config for PID module (rabbit messaging queue) ...')
        success = self.__iterate_over_all_hosts()
        msg = None
        if success:
            self.__loginfo('Config for PID module (rabbit messaging queue).. ok.')
            self.__loginfo('Successful connection to PID messaging queue at "%s".' % self.__current_rabbit_host)

        else:
            self.__loginfo('Config for PID module (rabbit messaging queue) .. FAILED!')
            msg = self.__assemble_error_message()

        if self.connection is not None:
            self.connection.close()
        return msg

    def __iterate_over_all_hosts(self):
        success = False
        print_conn = True
        print_chan = True
        self.channel_ok = False

        while True:
            try:
                if print_conn:
                    self.__loginfo(' .. checking authentication and connection ...')
                    print_conn = False
                    print_chan = True
                
                self.connection = self.__check_making_rabbit_connection()

                if print_chan:
                    self.__loginfo(' .. checking authentication and connection ... ok.')
                    self.__loginfo(' .. checking channel ...')
                    print_chan = False
                    print_conn = True

                self.channel_ok = False
                self.channel = self.__check_opening_channel(self.connection)
                self.channel_ok = True
                self.__check_exchange_existence(self.channel)

                if self.__send_message:
                    if self.__prefix is None:
                        # Cannot happen, as we get it from the connector object.
                        raise esgfpid.exceptions.ArgumentError('Missing handle prefix!')

                    self.__check_send_print_message(self.channel)

                success = True

                break # success, leave loop

            except ValueError as e:

                self.__loginfo(' .. giving this node a lower priority..')
                self.__nodemanager.set_priority_low_for_current()

                if self.__nodemanager.has_more_urls(): # stay in loop, try next host
                    utils.logtrace(LOGGER, 'Left URLs: %s', self.__nodemanager.get_num_left_urls())
                    self.__nodemanager.set_next_host()
                    self.__current_rabbit_host = self.__nodemanager.get_connection_parameters().host
                    utils.logtrace(LOGGER, 'Now trying: %s', self.__current_rabbit_host)

                else: # definitive fail, leave loop
                    break

        return success
            
    #
    # Building connections:
    #

    def __check_exchange_existence(self, channel):

        self.__exchange_name = self.__nodemanager.get_exchange_name()
        if self.__exchange_name is not None:
            try:
                self.__loginfo(' .. checking exchange ...')
                channel.exchange_declare(self.__exchange_name, passive=True)
                self.__loginfo(' .. checking exchange ... ok.')
            except (pika.exceptions.ChannelClosed) as e:
                self.__loginfo(' .. checking exchange ... failed.')
                self.__add_error_message_no_exchange()
                raise ValueError('The exchange %s does not exist on messaging service host %s' %
                    (self.__exchange_name, self.__current_rabbit_host))
        else:
            pass # No exchange name was given

    def __check_send_print_message(self, channel):
        props = pika.BasicProperties(
            delivery_mode = 2
        )
        
        # This also does the trick:
        #esgfpid.utils.routingkeys.add_prefix_to_routing_keys(self.__prefix)
        #rkey = utils.routingkeys.ROUTING_KEYS['pre_flight']
        rkey = utils.routingkeys.ROUTING_KEYS_TEMPLATES['pre_flight']
        sanitized_prefix = utils.routingkeys._sanitize_prefix(self.__prefix)
        rkey = rkey.replace('PREFIX', sanitized_prefix)
        if 'PREFIX' in rkey:
            raise ValueError('Prefix placeholder in routing key was not replaced!')

        body = 'PLEASE PRINT: Testing pre-flight check...'
        self.__loginfo(' .. checking message ...')
        res = channel.basic_publish(
            exchange=self.__nodemanager.get_exchange_name(),
            routing_key=rkey,
            body=body,
            properties=props,
            mandatory=True
        )
        if res:
            self.__loginfo(' .. checking message ... ok.')
        else:
            self.__loginfo(' .. checking message ... failed.')
            self.__add_error_message_message_fail(rkey)
            raise ValueError('Could not send message to messaging service host %s' %
                    (self.__current_rabbit_host))


    def __check_opening_channel(self, connection):
        channel = None
        try:
            channel = self.__open_channel(connection)
            self.__loginfo(' .. checking channel ... ok.')

        except pika.exceptions.ChannelClosed:
            self.__loginfo(' .. checking channel ... FAILED.')
            self.__add_error_message_channel_closed()
            raise ValueError('Channel failed, please try next.')

        return channel

    def __open_channel(self, connection):
        channel = connection.channel()
        channel.confirm_delivery()
        return channel

    def __check_making_rabbit_connection(self):
        connection = None
        try:
            connection = self.__open_rabbit_connection()

        except pika.exceptions.ProbableAuthenticationError:
            self.__loginfo(' .. checking authentication (%s)... FAILED.' % self.__current_rabbit_host)
            self.__add_error_message_authentication_error()
            raise ValueError('Connection failed, please try next.')

        except pika.exceptions.ProbableAccessDeniedError as e:
            self.__loginfo(' .. checking access (%s)... FAILED.' % self.__current_rabbit_host)
            vh = self.__nodemanager.get_connection_parameters().virtual_host
            if ('vhost %s not found' % vh) in str(e):
                self.__add_error_message_access_denied()
            else:
                self.__add_error_message_access_denied_unclear()
            raise ValueError('Access failed, please try next.')

        except (pika.exceptions.ConnectionClosed, socket.gaierror):
            self.__loginfo(' .. checking connection (%s)... FAILED.' % self.__current_rabbit_host)
            self.__add_error_message_connection_closed()
            raise ValueError('Connection failed, please try next.')
        
        if connection is None or not connection.is_open:
            self.__loginfo(' .. checking connection (%s)... FAILED.' % self.__current_rabbit_host)
            self.__add_error_message_connection_problem()
            raise ValueError('Connection failed, please try next.')

        self.__loginfo(' .. checking authentication and connection (%s)... ok.' % self.__current_rabbit_host)
        return connection

    def __open_rabbit_connection(self):
        params = self.__nodemanager.get_connection_parameters()
        self.__current_rabbit_host = params.host
        connection = self.__pika_blocking_connection(params)
        return connection

    def __pika_blocking_connection(self, params): # this is easy to mock
        return pika.BlockingConnection(params)

    #
    # Error messages
    #

    def __add_error_message_general(self):
        self.__error_messages.insert(0,'PROBLEM IN SETTING UP')
        self.__error_messages.insert(1,'RABBIT MESSAGING QUEUE (PID MODULE)')
        self.__error_messages.insert(2, 'CONNECTION TO THE PID MESSAGING QUEUE FAILED DEFINITIVELY:')
        self.__error_messages.append('PLEASE NOTIFY handle@dkrz.de AND INCLUDE THIS ERROR MESSAGE.')

    def __add_error_message_message_fail(self, rkey):
        msg = ' - host "%s": Message failure with routing key "%s".' % (self.__current_rabbit_host, rkey)
        self.__error_messages.append(msg)

    def __add_error_message_channel_closed(self):
        msg = ' - host "%s": Channel failure.' % self.__current_rabbit_host
        self.__error_messages.append(msg)

    def __add_error_message_access_denied(self):
        vh = self.__nodemanager.get_connection_parameters().virtual_host
        msg = ' - host "%s": Virtual host "%s" does not exist.' % (self.__current_rabbit_host, vh)
        self.__error_messages.append(msg)

    def __add_error_message_access_denied_unclear(self):
        msg = ' - host "%s": Access denied.' % (self.__current_rabbit_host)
        self.__error_messages.append(msg)

    def __add_error_message_no_exchange(self):
        msg = ' - host "%s": Exchange %s does not exist.' % (self.__current_rabbit_host, self.__exchange_name)
        self.__error_messages.append(msg)

    def __add_error_message_authentication_error(self):
        msg = (' - host "%s": Authentication failure (user %s, password %s).' % (
            self.__current_rabbit_host,
            self.__nodemanager.get_connection_parameters().credentials.username,
            self.__nodemanager.get_connection_parameters().credentials.password
        ))
        self.__error_messages.append(msg)

    def __add_error_message_connection_closed(self):
        msg = ' - host "%s": Connection failure (wrong host or port?).' % self.__current_rabbit_host
        self.__error_messages.append(msg)

    def __add_error_message_connection_problem(self):
        msg = ' - host "%s": Unknown connection failure.' % self.__current_rabbit_host
        self.__error_messages.append(msg)

    #
    # Inform at the end
    #

    def __assemble_error_message(self):
        self.__add_error_message_general()
        return utils.format_error_message(self.__error_messages)

    def __loginfo(self, msg):
        if self.__print_success_to_console == True:
            print(msg)
        utils.loginfo(LOGGER, msg)

    def __logwarn(self, msg):
        if self.__print_errors_to_console == True:
            print(msg)
        utils.logwarn(LOGGER, msg)


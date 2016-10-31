import logging
import pika
import Queue
import esgfpid.rabbit.connparams as connparams
import esgfpid.rabbit.rabbitutils as rabbitutils
import esgfpid.defaults as defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn, log_every_x_times

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class RabbitFeeder(object):

    def __init__(self, thread, statemachine, confirmer, exchange):
        self.thread = thread
        self.confirmer = confirmer
        self.statemachine = statemachine
        self.EXCHANGE = exchange

        ''' This is important. This defines the number of the message
        that is used to identify it, so the correct messages are deleted on
        confirm.
        It makes sure that rabbit server and this client talk about the
        same message.
        NEVER EVER INCREMENT OR OTHERWISE MODIFY THIS!

        From the docs:
        "The delivery tag is valid only within the channel from which
        the message was received. I.e. a client MUST NOT receive a
        message on one channel and then acknowledge it on another."
        Source: https://www.rabbitmq.com/amqp-0-9-1-reference.html
        '''
        self.__message_number = 0

        ''' Queue that will contain the unpublished messages'''
        self.__unpublished_messages_queue = Queue.Queue()

        # Own private attributes
        self.__first_carrot_trigger = True
        self.__have_not_warned_about_connection_fail_yet = True # TODO HERE?
        self.__have_not_warned_about_force_close_yet = True # TODO HERE?
        self.lc = 1
        self.li = 10

    def publish_message(self):
        if self.__first_carrot_trigger:
            logdebug(LOGGER, 'Received first trigger for feeding the rabbit.')
            self.__first_carrot_trigger = False
        logtrace(LOGGER, 'Received trigger for feeding the rabbit.')
        if self.statemachine.is_available_for_server_communication(): # TODO This may be redundant, as it was already checked during trigger!
            log_every_x_times(LOGGER, self.lc, self.li, 'Received trigger for feeding the rabbit')
            logtrace(LOGGER, 'Publishing module is ready for feeding the rabbit.')
            self.__try_and_catch()
        else:
            self.__inform_why_cannot_feed_the_rabbit()

    def put_message_into_queue_of_unsent_messages(self, message):
        self.__unpublished_messages_queue.put(message, block=False)

    def __inform_why_cannot_feed_the_rabbit(self):
        log_every_x_times(LOGGER, self.lc, self.li, 'Cannot feed the rabbit')
        msg = 'Cannot feed carrot to rabbit'
        if self.statemachine.is_waiting_to_be_available():
            logdebug(LOGGER, msg+' yet, as the connection is not ready.')

        elif self.statemachine.is_not_started_yet():
            logdebug(LOGGER, msg+' yet, as the thread is not running yet.')

        elif self.statemachine.is_permanently_unavailable():

            if self.statemachine.could_not_connect:
                logtrace(LOGGER, msg+', as the connection failed.')
                if self.__have_not_warned_about_connection_fail_yet:
                    logwarn(LOGGER, msg+'. The connection failed definitively.')
                    self.__have_not_warned_about_connection_fail_yet = False

            elif self.statemachine.closed_by_publisher:
                logtrace(LOGGER, msg+', as the connection was closed by the user.')
                if self.__have_not_warned_about_force_close_yet:
                    logwarn(LOGGER, msg+'. The sender was force closed.')
                    self.__have_not_warned_about_force_close_yet = False

        else: # TODO when do these happen?
            if self.thread._channel is None:
                logwarn(LOGGER, msg+' There is no channel.')

    def __try_and_catch(self):
        try:
            self.__try_feeding_next_carrot()
        except pika.exceptions.ChannelClosed, e:
            logwarn(LOGGER, 'Cannot feed carrot %i because the Channel is closed (%s)', self.__message_number+1, e.message)
        except AttributeError, e:
            if self.thread._channel is None:
                logwarn(LOGGER, 'Cannot feed carrot %i, because there is no channel.', self.__message_number+1)
            else:
                logwarn(LOGGER, 'Cannot feed carrot %i (unexpected error %s)', self.__message_number+1, e.message)
        except Queue.Empty, e:
            logtrace(LOGGER, 'Queue empty. No more carrots to be fed.')
        except AssertionError as e:
            logwarn(LOGGER, 'Cannot feed carrot %i because of AssertionError: "%s"', self.__message_number+1,e)
            if e.message == 'A non-string value was supplied for self.exchange':
                logwarn(LOGGER, 'Exchange was "%s" (type %s)',self.EXCHANGE, type(self.EXCHANGE))
            # TODO How to make sure the publish is called exactly as many times as messages were added?

    def __try_feeding_next_carrot(self):
        properties = connparams.get_properties_for_message_publications()
        msg = self.__get_carrot_to_feed()
        success = False
        try:
            routing_key, msg_string = rabbitutils.get_routing_key_and_string_message_from_message_if_possible(msg)
            logtrace(LOGGER, 'Feeding carrot %i (key %s) (body %s)...', self.__message_number+1, routing_key, msg_string) # +1 because it will be incremented after the publish.
            self.__actual_publish_to_channel(msg_string, properties, routing_key)
            success = True
        except Exception as e:
            success = False
            logwarn(LOGGER, 'Carrot was not fed. Putting back to queue. Error: "%s"',e)
            self.put_message_into_queue_of_unsent_messages(msg)
            logtrace(LOGGER, 'Now (after putting back) left in queue to be fed: %i carrots.', self.__unpublished_messages_queue.qsize())
            raise e
        if success:
            self.__postparations_after_successful_feeding(msg, msg_string)

    def __get_carrot_to_feed(self, seconds=2):
        msg = self.__unpublished_messages_queue.get(block=True, timeout=seconds) # can raise Queue.Empty
        logtrace(LOGGER, 'Found carrot to feed. Now left in queue to be fed: %i carrots.', self.__unpublished_messages_queue.qsize())
        return msg

    def __actual_publish_to_channel(self, msg_string, properties, routing_key):
        log_every_x_times(LOGGER, self.lc, self.li, 'Actual publish to channel no. %i.', self.thread._channel.channel_number)
        self.thread._channel.basic_publish(
            exchange=self.EXCHANGE,
            routing_key=routing_key,
            body=msg_string,
            properties=properties,
            mandatory=defaults.RABBIT_MANDATORY_DELIVERY
        )

    def __postparations_after_successful_feeding(self, msg, msg_string):
        log_every_x_times(LOGGER, self.lc, self.li, 'Actual publish to channel done')
        self.__message_number += 1 # IMPORTANT: This has to be incremented BEFORE we use it as delivery tag etc.!
        self.confirmer.put_to_unconfirmed_delivery_tags(self.__message_number)
        self.confirmer.put_to_unconfirmed_messages_dict(self.__message_number, msg)
        loginfo(LOGGER, 'Message sent (no. %i).', self.__message_number)
        logtrace(LOGGER, 'Feeding carrot %i (%s)... done.',
            self.__message_number,
            msg_string)

    def get_num_unpublished(self):
        return self.__unpublished_messages_queue.qsize()

    def get_unpublished_messages_as_list_copy(self):
        newlist = []
        counter = 0
        to_be_safe = 10
        max_iterations = self.__unpublished_messages_queue.qsize() + to_be_safe
        while True:

            counter+=1
            if counter == max_iterations:
                break

            try:
                self.__get_msg_from_queue_and_store_first_try(
                    newlist,
                    self.__unpublished_messages_queue)
            except Queue.Empty:
                try:
                    self.__get_a_msg_from_queue_and_store_second_try(
                        newlist,
                        self.__unpublished_messages_queue)
                except Queue.Empty:
                    break
        return newlist

    def __get_msg_from_queue_and_store_first_try(self, alist, queue):
        msg_incl_routing_key = queue.get(block=False)
        alist.append(msg_incl_routing_key)

    def __get_a_msg_from_queue_and_store_second_try(self, alist, queue):
        wait_seconds = 0.5
        msg_incl_routing_key = queue.get(block=True, timeout=wait_seconds)
        alist.append(msg_incl_routing_key)

    def reset_message_number(self):
        # CAREFUL
        # This can be called only on a reconnection!
        self.__message_number = 0
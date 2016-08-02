import json
import logging
import mock
import sys
sys.path.append('..')
import esgfpid

class SimpleMockRabbitSender(object):

    def __init__(self):
        self.routing_key = None
        self.received_messages = []

    def __remove_date(self, msg):
        if 'message_timestamp' in msg:
            msg['message_timestamp'] = 'anydate'
        return msg

    def send_message_to_queue(self, message):
        self.received_messages.append(self.__remove_date(message))

    def open_rabbit_connection(self):
        pass

    def close_rabbit_connection(self):
        pass

    def finish(self):
        pass

    def start(self):
        pass

    def force_finish(self):
        pass

class MockRabbitSender(object):

    def __init__(self):
        self.routing_key = None
        self.received_messages = []
        self.count = 0

    def __remove_date(self, msg):
        msg['message_timestamp'] = 'anydate'
        return msg

    def send_message_to_queue(self, **args):
        mandatory_args = ['routing_key', 'message']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)
        self.routing_key = args['routing_key']
        msg = args['message']
        self.received_messages.append(self.__remove_date(msg))

        # Retrieve testcase identifier and test message identifier
        #testcase_identifier = '(no testcase identifier)'
        #try:
        #    testcase_identifier = json.loads(msg)['testcase_identifier']
        #except (TypeError):
        #    testcase_identifier = msg['testcase_identifier']            
        # 
        #testmessage_identifier = '(no testmessage identifier)'
        #try:
        #    testmessage_identifier = json.loads(msg)['testmessage_identifier']
        #except (TypeError):
        #    testmessage_identifier = msg['testmessage_identifier']

        self.count = self.count + 1
        self.received_messages.append(msg)
        #if testcase_identifier is None:
        #    raise ValueError('None1!')
        #if testcase_identifier is None:
        #    raise ValueError('None2!')
        #if testcase_identifier is None:
        #    raise ValueError('None3!')
        #messagelogger.info(testcase_identifier+'#'+testmessage_identifier+'#'+json.dumps(msg))

    def open_rabbit_connection(self):
        pass

    def close_rabbit_connection(self):
        pass

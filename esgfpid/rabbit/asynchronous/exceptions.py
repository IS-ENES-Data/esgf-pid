class ConnectionNotReady(Exception):

    def __init__(self, custom_message=None):
        self.msg = 'Connection to rabbit not ready.'
        self.custom_message = custom_message

        if self.custom_message is not None:
            self.msg += ': '+self.custom_message
        self.msg += '.'

        super(self.__class__, self).__init__(self.msg)

class OperationNotAllowed(Exception):

    def __init__(self, custom_message=None, op=None):

        # Operation:
        if op is None:
            op=''
        else:
            op='"%s" ' % op

        # Message
        self.msg = 'Operation %snot allowed right now' % op

        # Append custom message
        if custom_message is not None:
            self.msg += ': '+custom_message
        self.msg += '.'

        super(self.__class__, self).__init__(self.msg)

class UnknownServerResponse(Exception):

    def __init__(self, custom_message=None):
        self.msg = 'Cannot understand RabbitMQ\'s response'
        self.custom_message = custom_message

        if self.custom_message is not None:
            self.msg += ': '+self.custom_message
        self.msg += '.'

        super(self.__class__, self).__init__(self.msg)
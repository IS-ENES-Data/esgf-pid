class PIDServerException(Exception):

    def __init__(self, custom_message):
        super(self.__class__, self).__init__(custom_message)
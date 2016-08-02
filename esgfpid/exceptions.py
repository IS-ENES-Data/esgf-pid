
class OperationUnsupportedException(Exception):

    def __init__(self, custom_message=None):
        self.msg = 'Operation not supported at this point'
        self.custom_message = custom_message

        if self.custom_message is not None:
            self.msg += ': '+self.custom_message
        self.msg += '.'

        super(self.__class__, self).__init__(self.msg)

class InconsistentFilesetException(Exception):

    def __init__(self, custom_message=None):
        self.msg = 'Fileset is inconsistent with previously published copy'
        self.custom_message = custom_message

        if self.custom_message is not None:
            self.msg += ': '+self.custom_message
        self.msg += '.'

        super(self.__class__, self).__init__(self.msg)

class ArgumentError(Exception):

    def __init__(self, custom_message=None):
        self.msg = 'The arguments that were passed are not ok'
        self.custom_message = custom_message

        if self.custom_message is not None:
            self.msg += ': '+self.custom_message
        self.msg += '.'

        super(self.__class__, self).__init__(self.msg)

class ESGFException(Exception):

    def __init__(self, custom_message=None):
        self.msg = 'ESGF rule violation'
        self.custom_message = custom_message

        if self.custom_message is not None:
            self.msg += ': '+self.custom_message
        self.msg += '.'

        super(self.__class__, self).__init__(self.msg)

class SolrSwitchedOff(Exception):

    def __init__(self, custom_message=None):
        self.msg = 'Solr module is switched off'
        self.custom_message = custom_message

        if self.custom_message is not None:
            self.msg += ': '+self.custom_message
        self.msg += '.'

        super(self.__class__, self).__init__(self.msg)


class SolrError(Exception):

    def __init__(self, custom_message=None):
        self.msg = 'Error during communication with solr'
        self.custom_message = custom_message

        if self.custom_message is not None:
            self.msg += ': '+self.custom_message
        self.msg += '.'

        super(self.__class__, self).__init__(self.msg)

class SolrResponseError(Exception):

    def __init__(self, custom_message=None):
        self.msg = 'Error parsing solr response'
        self.custom_message = custom_message

        if self.custom_message is not None:
            self.msg += ': '+self.custom_message
        self.msg += '.'

        super(self.__class__, self).__init__(self.msg)
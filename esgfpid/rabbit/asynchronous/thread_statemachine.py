

class StateMachine(object):
    
    def __init__(self):

        # The four main states of the state machine:
        self.__NOT_STARTED_YET = 0
        self.__WAITING_TO_BE_AVAILABLE = 1
        self.__IS_AVAILABLE = 2
        self.__IS_AVAILABLE_BUT_WANTS_TO_STOP = 3
        self.__PERMANENTLY_UNAVAILABLE = 4 # roughly corresponds to _stopping and _closing in pika usage example.

        # Init state machine:
        self.__state = self.__NOT_STARTED_YET

        # More detail
        self.detail_closed_by_publisher = False
        self.detail_asked_to_closed_by_publisher = False
        self.detail_could_not_connect = False
        self.detail_authentication_exception = False

    #
    # Setters
    #

    ''' Called by the rabbit thread.'''
    def set_to_available(self):
        self.__state = self.__IS_AVAILABLE

    ''' Called by the main thread.'''
    def set_to_wanting_to_stop(self):
        self.__state = self.__IS_AVAILABLE_BUT_WANTS_TO_STOP

    ''' Called by the main thread.'''
    def set_to_waiting_to_be_available(self):
        self.__state = self.__WAITING_TO_BE_AVAILABLE

    ''' Called by the rabbit thread.'''
    def set_to_permanently_unavailable(self):
        self.__state = self.__PERMANENTLY_UNAVAILABLE

    #
    # Getters for states
    #

    def is_NOT_STARTED_YET(self):
        if self.__state == self.__NOT_STARTED_YET:
            return True
        return False

    def is_WAITING_TO_BE_AVAILABLE(self):
        if self.__state == self.__WAITING_TO_BE_AVAILABLE:
            return True
        return False

    def is_AVAILABLE(self):
        if self.__state == self.__IS_AVAILABLE:
            return True
        return False

    def is_AVAILABLE_BUT_WANTS_TO_STOP(self):
        if self.__state == self.__IS_AVAILABLE_BUT_WANTS_TO_STOP:
            return True
        return False

    def is_PERMANENTLY_UNAVAILABLE(self):
        if self.__state == self.__PERMANENTLY_UNAVAILABLE:
            return True
        return False

    #
    # Combined getters
    # (convenience wrappers)
    #

    def is_available_for_client_publishes(self):
        return self.__IS_AVAILABLE

    def is_available_for_server_communication(self):
        if self.__IS_AVAILABLE:
            return True
        if self.__IS_AVAILABLE_BUT_WANTS_TO_STOP:
            return True
        return False








class StateMachine(object):
    
    def __init__(self):

        # The four main states of the state machine:
        self.__NOT_STARTED_YET = 0
        self.__IS_AVAILABLE = 1
        self.__IS_AVAILABLE_BUT_WANTS_TO_STOP = 5
        self.__WAITING_TO_BE_AVAILABLE = 2
        self.__PERMANENTLY_UNAVAILABLE = 3 # roughly corresponds to _stopping and _closing in pika usage example.
        # Init state machine:
        self.__state = self.__NOT_STARTED_YET

        # More detail
        self.closed_by_publisher = False
        self.asked_to_closed_by_publisher = False
        self.could_not_connect = False

    def is_available_but_wants_to_stop(self):
        if self.__state == self.__IS_AVAILABLE_BUT_WANTS_TO_STOP:
            return True
        return False

    def is_available_for_client_publishes(self):
        if self.__state == self.__IS_AVAILABLE:
            return True
        return False

    def is_available_for_server_communication(self):
        if (self.__state == self.__IS_AVAILABLE or
            self.__state == self.__IS_AVAILABLE_BUT_WANTS_TO_STOP):
            return True
        return False

    def is_not_started_yet(self):
        if self.__state == self.__NOT_STARTED_YET:
            return True
        return False

    def is_waiting_to_be_available(self):
        if self.__state == self.__WAITING_TO_BE_AVAILABLE:
            return True
        return False

    def is_permanently_unavailable(self):
        if self.__state == self.__PERMANENTLY_UNAVAILABLE:
            return True
        return False

    def set_to_available(self):
        self.__state = self.__IS_AVAILABLE

    def set_to_wanting_to_stop(self):
        self.__state = self.__IS_AVAILABLE_BUT_WANTS_TO_STOP

    def set_to_waiting_to_be_available(self):
        self.__state = self.__WAITING_TO_BE_AVAILABLE

    def set_to_permanently_unavailable(self):
        self.__state = self.__PERMANENTLY_UNAVAILABLE
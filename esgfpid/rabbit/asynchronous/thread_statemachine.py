

class StateMachine(object):
    
    def __init__(self):

        # The four main states of the state machine:
        self.__NOT_STARTED_YET = 0
        self.__WAITING_TO_BE_AVAILABLE = 1
        self.__IS_AVAILABLE = 2
        self.__IS_AVAILABLE_BUT_WANTS_TO_STOP = 3
        self.__PERMANENTLY_UNAVAILABLE = 4 # roughly corresponds to _stopping and _closing in pika usage example.
        self.__FORCE_FINISHED = 5

        # Init state machine:
        self.__state = self.__NOT_STARTED_YET

        # More detail
        self.__detail_closed_by_publisher = False # this needs a setter, as it depends on the others!
        self.detail_asked_to_force_close_by_publisher = False
        self.detail_asked_to_gently_close_by_publisher = False
        self.detail_could_not_connect = False

    #
    # Setters
    #

    ''' Called by the rabbit thread.'''
    def set_to_available(self):
        if self.is_PERMANENTLY_UNAVAILABLE() or self.is_FORCE_FINISHED():
            pass
        else:
            self.__state = self.__IS_AVAILABLE

    ''' Called by the main thread.'''
    def set_to_wanting_to_stop(self):
        if self.is_PERMANENTLY_UNAVAILABLE() or self.is_FORCE_FINISHED():
            pass
        else:
            self.__state = self.__IS_AVAILABLE_BUT_WANTS_TO_STOP

    ''' Called by the main thread.'''
    def set_to_waiting_to_be_available(self):
        if self.is_PERMANENTLY_UNAVAILABLE() or self.is_FORCE_FINISHED():
            pass
        else:
            self.__state = self.__WAITING_TO_BE_AVAILABLE

    ''' Called by the rabbit thread.'''
    def set_to_permanently_unavailable(self):
        if self.is_FORCE_FINISHED():
            pass
        else:
            self.__state = self.__PERMANENTLY_UNAVAILABLE

    def set_to_force_finished(self):
        self.__state = self.__FORCE_FINISHED

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
        if self.__state == self.__PERMANENTLY_UNAVAILABLE or self.__state == self.__FORCE_FINISHED:
            # Including FORCE_FINISHED is kind of a dirty hack here,
            # but otherwise I might break to many things...
            # This hack should not be necessary anymore.
            return True
        return False

    def is_FORCE_FINISHED(self):
        if self.__state == self.__FORCE_FINISHED:
            return True
        return False


    '''
    Needed by asynchronous.py to inform if messages
    are not accepted anymore.
    '''
    def get_reason_shutdown(self):
        if self.detail_could_not_connect:
            return 'Could not connect'
        elif self.__detail_closed_by_publisher or self.detail_asked_to_gently_close_by_publisher or self.detail_asked_to_force_close_by_publisher:
            return 'Was closed by publisher'
            # The former two are more important, so only if no
            # authentication exception or could-not-connect occurred,
            # we give user close as the reason.
        else:
            return 'Unknown'

    #
    # Detailed setters
    #

    def set_detail_closed_by_publisher(self):
        if self.detail_could_not_connect:
            # If the connection was already closed because of these,
            # it was not really closed by publisher!!
            pass
        else:
            self.__detail_closed_by_publisher = True

    def get_detail_closed_by_publisher(self):
        return self.__detail_closed_by_publisher
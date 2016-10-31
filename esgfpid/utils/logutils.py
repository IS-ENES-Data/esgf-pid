import esgfpid.defaults

#
# Logging helpers
#

def logtrace(logger, msg, *args, **kwargs):
    '''
    If esgfpid.defaults.LOG_TRACE_TO_DEBUG, messages are treated
    like debug messages (with an added [trace]).
    Otherwise, they are ignored.
    '''
    if esgfpid.defaults.LOG_TRACE_TO_DEBUG:
        logdebug(logger, '[trace] %s' % msg, *args, **kwargs)
    else:
        pass

def logdebug(logger, msg, *args, **kwargs):
    '''
    Logs messages as DEBUG,
    unless show=True and esgfpid.defaults.LOG_SHOW_TO_INFO=True,
    (then it logs messages as INFO).
    '''
    if _is_show(kwargs):
        _logshow(logger, msg, *args, **kwargs)
    elif esgfpid.defaults.LOG_DEBUG_TO_INFO:
        logger.info('DEBUG %s ' % msg, *args, **kwargs)
    else:
        logger.debug(msg, *args, **kwargs)

def _is_show(kwargs):
    show_this_message = ('show' in kwargs) and (kwargs['show'] == True)
    kwargs.pop('show', None) # Need to remove show=xyz before passing kwargs to logger!
    if show_this_message and esgfpid.defaults.LOG_SHOW_TO_INFO:
        return True
    return False

def _logshow(logger, msg, *args, **kwargs):
    kwargs.pop('show', None) # Need to remove show=xyz before passing kwargs to logger!
    logger.info(msg, *args, **kwargs)

def loginfo(logger, msg, *args, **kwargs):
    '''
    Logs messages as INFO,
    unless esgfpid.defaults.LOG_INFO_TO_DEBUG,
    (then it logs messages as DEBUG).
    '''
    if _is_show(kwargs):
        _logshow(logger, msg, *args, **kwargs)
    elif esgfpid.defaults.LOG_INFO_TO_DEBUG:
        logger.debug(msg, *args, **kwargs)
    else:
        logger.info(msg, *args, **kwargs)


def logwarn(logger, msg, *args, **kwargs):
    if _is_show(kwargs):
        _logshow(logger, '[WARN] %s' % msg, *args, **kwargs)
    else:
        logger.warn(msg, *args, **kwargs)

def logerror(logger, msg, *args, **kwargs):
    if _is_show(kwargs):
        _logshow(logger, '[ERROR] %s' % msg, *args, **kwargs)
    else:
        logger.error(msg, *args, **kwargs)

def log_every_x_times(logger, counter, x, msg, *args, **kwargs):
    '''
    Works like logdebug, but only prints first and
    and every xth message.
    '''
    if counter==1 or counter % x == 0:
        #msg = msg + (' (counter %i)' % counter)
        logdebug(logger, msg, *args, **kwargs)

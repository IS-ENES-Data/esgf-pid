import esgfpid.defaults
import copy

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
    if esgfpid.defaults.LOG_DEBUG_TO_INFO:
        logger.info('DEBUG %s ' % msg, *args, **kwargs)
    else:
        logger.debug(msg, *args, **kwargs)

def loginfo(logger, msg, *args, **kwargs):
    '''
    Logs messages as INFO,
    unless esgfpid.defaults.LOG_INFO_TO_DEBUG,
    (then it logs messages as DEBUG).
    '''
    if esgfpid.defaults.LOG_INFO_TO_DEBUG:
        logger.debug(msg, *args, **kwargs)
    else:
        logger.info(msg, *args, **kwargs)


def logwarn(logger, msg, *args, **kwargs):
    logger.warn(msg, *args, **kwargs)

def logerror(logger, msg, *args, **kwargs):
    logger.error(msg, *args, **kwargs)

def log_every_x_times(logger, counter, x, msg, *args, **kwargs):
    '''
    Works like logdebug, but only prints first and
    and every xth message.
    '''
    if counter==1 or counter % x == 0:
        #msg = msg + (' (counter %i)' % counter)
        logdebug(logger, msg, *args, **kwargs)

def make_logsafe(list_or_dict, forbidden='password'):
    if not forbidden in str(list_or_dict):
        return list_or_dict

    # don't modify the original:
    logsafe = copy.deepcopy(list_or_dict)

    # Dict (only first level):
    # TODO: Recursively check deeper levels, or count occurrences of 
    # forbidden word to make sure you got them all.
    try:
        logsafe[forbidden] = logsafe[forbidden][0:3]+'...'
    except KeyError:
        pass
    except TypeError:

        # List of dicts (only first level):
        for item in logsafe:
            try:
                item[forbidden] = item[forbidden][0:3]+'...'
            except KeyError:
                pass

    return logsafe


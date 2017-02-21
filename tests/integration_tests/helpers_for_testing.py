'''
Module that provides some helpers and variables
for testing the esgfpid.rabbit.asynchronous module.

It comes with a test of its own method. Test it 
by running this module as a python script.

'''
import logging
import datetime


# Load RabbitMQ nodes from the credentials file:
from rabbitcredentials_SECRET import *

# Load module containing the methods to connect
# with RabbitMQ...
'''
To test using the entire library! This uses the module
esgfpid.connector, just like the publierh, and creates
data_cart pids.
'''
###from helpers_esgfpid import * # Use with care! Created PIDs!
'''
To test using only the esgfpid.rabbit module.
This creates messages that are NOT understood by
any consumer, so no PIDs will be created!
'''
from helpers_module import *

# Create the test node info:
node1_ok = {'user':'esgf-publisher', 'password':password_node1, 'url':url_node1, 'priority':1}
node2_ok = {'user':'esgf-publisher', 'password':password_node2, 'url':url_node2, 'priority':2}
node1_wrongpassword = {'user':'esgf-publisher', 'password':'foo', 'url':url_node1, 'priority':1}
node2_wrongpassword = {'user':'esgf-publisher', 'password':'foo', 'url':url_node2, 'priority':2}
node3_inexistent = {'user':'esgf-publisher', 'password':'foo', 'url':'notexist.dkrz.de',       'priority':1}
node4_inexistent = {'user':'esgf-publisher', 'password':'foo', 'url':'notexist.pcmdi.gov',     'priority':2}

# Prepare logging:
LOGLEVEL = logging.DEBUG
LOGGER_ALL = logging.getLogger('esgfpid')
LOGGER_ALL.setLevel(logging.WARNING)
pikalogger = logging.getLogger('pika')
pikalogger.setLevel(logging.WARNING)
LOGPATH = 'logs'
esgfpid.utils.ensure_directory_exists(LOGPATH)
LOGDATE = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
myformat = '%(asctime)-15s %(levelname)-5s: %(name)s [%(threadName)s]: %(message)s'
FORMATTER = logging.Formatter(myformat)

# Helper functions:

'''
Changes the file that the log statements
are written into.

:param num: Test number to include in the file name.
:return: The entire file name the log statements will be written to.
'''
def init_logging(num):
    LOGGER = None
    LOGGER = logging.getLogger('esgfpid')
    LOGGER.handlers = []
    LOGGER.setLevel(LOGLEVEL)
    filename = LOGPATH+'/log_for_test_'+str(num)+'__'+LOGDATE+'_'+logging.getLevelName(LOGLEVEL)+'.log'
    handler = logging.FileHandler(filename, mode='w')
    handler.setFormatter(FORMATTER)
    LOGGER.addHandler(handler)
    #print 'Logging to file ".%s"' % filename
    return filename

'''
Simply sleep for some seconds and print this to
stdout, for the watching user to see.

:param seconds: For how much time to wait.
'''
def wait(seconds):
    print('Waiting %i seconds...' % seconds)
    time.sleep(seconds)

'''
Helper for all_lines_found().

Reads a file line by line. Stops (and returns True)
when the passed string is found. Returns False if EOF
if reached without finding the string.

Modifies the read-position of the logfile opened in
all_lines_found(), so that the next call continues
looking at the same position.
'''
def _read_lines_until_found_string(logfile, onestring):
    while True:
        line = logfile.readline()
        if len(line)==0:
            print('***** Missing line: "%s"' % onestring)
            return False
        elif onestring in line:
            print('Found: "%s"' % onestring)
            return True

'''
Helper for all_lines_found().

Reads a file line by line. Stops (and returns True)
when one of the passed strings is found. Returns False if
EOF if reached without finding the string.
'''
def _read_lines_until_found_one_string(logfile, list_of_alternatives):
    while True:
        line = logfile.readline()
        if len(line)==0:
            print('***** Missing line: One of "%s"' % list_of_alternatives)
            return False

        for onestring in list_of_alternatives:
            if onestring in line:
                print('Found: "%s"' % onestring)
                return True

'''
Helper
'''
def _is_string_in_line(candidate, line):
    if candidate in line:
        print('Found: "%s"' % candidate)
        return True
    else:
        return False
'''
Helper
'''
def _is_one_string_in_line(list_of_candidates, line):
    for candidate in list_of_candidates:
        if candidate in line:
            print('Found: "%s"' % candidate)
            return True
    return False

def all_lines_found(list_of_strings, logfilename):
    num_not_found = 0
    num_found = 0
    num_to_find = len(list_of_strings)
    print("Searching file for %i specified lines..." % num_to_find)

    # Open log file to search:
    with open(logfilename, 'r') as logfile:

        # First item to find:
        to_find = list_of_strings.pop(0)
        while True:
            line = logfile.readline()

            # If we reach EOF without finding the string,
            # it is missing:
            if len(line)==0:
                if type(to_find) == type('foo'):
                    print('***** Missing line: "%s"' % to_find)
                else:
                    print('***** Missing line: One of "%s"' % to_find)
                num_not_found += 1
                break # break while loop

            # If there is another line, check if the item
            # was found:
            else:
                found = False
                if type(to_find) == type('foo'):
                    found = _is_string_in_line(to_find, line)
                elif type(to_find) == type([]):
                    found = _is_one_string_in_line(to_find, line)
                else:
                    raise TypeError('Wrong type (%s)! Accept only strings or list of strings. Got: %s' % (type(item), item))

                # If item is found, prepare looking for next item:
                if found:
                    num_found += 1
                    if len(list_of_strings) == 0:
                        break # break while loop
                    else:
                        to_find = list_of_strings.pop(0)
                    

    # Finally, print results:
    if num_not_found > 0:
        num_not_checked = num_to_find - num_found - num_not_found
        if num_not_checked == 0:
            print("Bad:  Found %i/%i. Last one missing." % (num_found, num_to_find))
        else:
            print("Bad:  Found %i/%i. Missing: %i/%i. Not checked: %i/%i" % (num_found, num_to_find, num_not_found, num_to_find, num_not_checked, num_to_find))
        return False
    else:
        print('Good: Found %i/%i.' % (num_found, num_to_find))
        return True


'''
This method iterates over the lines of a text file and
looks for specified lines in the specified order.

Order matters!

As soon as one line is not found, the function exits,
the remaining lines are not looked for anymore.

If you look for one-of-several strings, pass them as
a list. So you pass a list of things-to-look-for, which
can be strings, or lists of strings.

The file to search is passed as a filename (string).
'''
def all_lines_found_OLD(list_of_strings, filename):
    num_not_found = 0
    num_found = 0
    num_to_find = len(list_of_strings)
    print("Searching file for %i specified lines..." % num_to_find)
    with open(filename, 'r') as logfile:
        for item in list_of_strings:

            # Check if the item is found:
            found = False
            if type(item) == type('foo'):
                found = _read_lines_until_found_string(logfile, item)
            elif type(item) == type([]):
                found = _read_lines_until_found_one_string(logfile, item)
            else:
                raise TypeError('Wrong type (%s)! Accept only strings or list of strings. Got: %s' % (type(item), item))

            # Increment numbers
            if found:
                num_found += 1
            else:
                num_not_found += 1
                break

    if num_not_found > 0:
        num_not_checked = num_to_find - num_found - num_not_found
        if num_not_checked == 0:
            print("Bad:  Found %i/%i. Last one missing." % (num_found, num_to_find))
        else:
            print("Bad:  Found %i/%i. Missing: %i/%i. Not checked: %i/%i" % (num_found, num_to_find, num_not_found, num_to_find, num_not_checked, num_to_find))
        return False
    print('Good: Found %i/%i.' % (num_found, num_to_find))
    return True

def _test_all_lines_found():
    filename = 'testlog_dont_delete.log'

    search = ['AAA','CCC','DDD']
    print('\nLooking for %s!' % search)
    if not all_lines_found(search, filename):
        raise ValueError('Method "all_lines_found" does not work properly! (%s)' % search)

    search = ['AAA','BBB','CCC','DDD','EEE']
    print('\nLooking for %s!' % search)
    if not all_lines_found(search, filename):
        raise ValueError('Method "all_lines_found" does not work properly! (%s)' % search)
    
    search = ['AAA',['BBB','YYY'],'DDD']
    print('\nLooking for %s!' % search)
    if not all_lines_found(search, filename):
        raise ValueError('Method "all_lines_found" does not work properly! (%s)' % search)

    search = ['AAA','YYY','BBB','CCC','DDD']
    print('\nLooking for %s!' % search)
    if all_lines_found(search, filename):
        raise ValueError('Method "all_lines_found" does not work properly! (%s)' % search)

    search = ['AAA',['YYY','ZZZ']]
    print('\nLooking for %s!' % search)
    if all_lines_found(search, filename):
        raise ValueError('Method "all_lines_found" does not work properly! (%s)' % search)

    search = ['AAA',['YYY','ZZZ'], 'DDD', 'EEE']
    print('\nLooking for %s!' % search)
    if all_lines_found(search, filename):
        raise ValueError('Method "all_lines_found" does not work properly! (%s)' % search)

_test_all_lines_found()
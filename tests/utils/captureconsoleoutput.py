'''
This module provides a function that allows to capture
messages printed to stout for checking if the error
messages got printed.

The idea is from Stack Overflow:
http://stackoverflow.com/questions/4219717/how-to-assert-output-with-nosetest-unittest-in-python

'''

from contextlib import contextmanager
import sys
if sys.version[0] == '2':
    from StringIO import StringIO
else:
    from io import StringIO

@contextmanager
def captured_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err
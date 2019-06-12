'''
A script to test all tasks of the esgfpid library.

This only tests the most basic standard cases.

Please checkout the logs to verify the correct
execution of all functionality. As the module
has two threads and some parallel, asynchronous
execution, errors tend not to show in this script.

'''

from __future__ import print_function

from helpers_for_testing import *

#
# Test cases
#
run_cases = [-1]
not_pass = []


# CASE -1
# Needs human interaction!
# Interrupting RabbitMQ... One alternative.
num = -1
if num in run_cases:
    print("\n\nCASE %i" % num)
    logfile = init_logging(num)
    try:
        node3 = {'user':'esgf-publisher', 'password':password_node1, 'url':'handle-esgf-trusted.dkrz.de', 'priority':2} # TODO
        connector = init_connector([node1_ok, node3])
        print("\n\tLog into RabbitMQ host\n\n\tRun\n\tservice rabbitmq-server stop\n\tAFTER starting to send messages... You have some waiting now and then 10 seconds of sending messages.")
        wait(6)
        send_messages(20, connector, wait=0.5)
        wait(1)
        gentle_close(connector)
    except Exception as e:
        print('Caught!')
    print("\n\tLog into RabbitMQ host\n\n\tRun\n\tservice rabbitmq-server start\n\tservice handle restart")
    lines = []
    # (1) Connecting to rabbit. This must succeed for the test to do what we want to test!
    lines.append('Opening connection to RabbitMQ...')
    lines.append('Connection to RabbitMQ at pid1.dkrz.de opened...')
    lines.append('Opening channel... done.')
    lines.append('Set confirm delivery... done.')
    lines.append('Setup is finished. Publishing may start.')
    # (2) We want the connection to be totally ready when the messages start
    lines.append('Ready to publish messages to RabbitMQ. No messages waiting yet.')
    # (3) Rabbit fails...
    lines.append(['Connection to RabbitMQ was closed. Reason: Not specified.',
                  "Connection to RabbitMQ was closed. Reason: CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'."])
    # (4) Trying to reconnect...
    lines.append('Connection failure: 1 fallback URLs left to try.')
    lines.append('Connection failure: Trying to connect (now) to handle-esgf-trusted.dkrz.de.')
    lines.append('Trying to reconnect to RabbitMQ in 0 seconds.')
    # (5) Reconnect succeeds...
    lines.append('Reconnect: Sending all messages that have not been confirmed yet...') # Strange order...
    lines.append('Opening connection to RabbitMQ...')
    # (6) Send messages
    lines.append('Connection to RabbitMQ at handle-esgf-trusted.dkrz.de opened...')
    lines.append('(Re)connection established, making ready for publication...')
    lines.append('messages are already waiting to be published.')
    # (7) Gentle close
    lines.append(['Gentle finish (iteration 0): Some pending messages left.',
                  'Gentle finish (iteration 0): No more pending messages.'])
    # (8) At the end, the thread is joined:
    lines.append('All messages sent and confirmed. Closing.')
    lines.append('[MainThread]: Joining... done')
    if not all_lines_found(lines, logfile):
        not_pass.append(num)

# CASE 0
# Needs human interaction!
# And blocks, because of PID Exception!
# Interrupting RabbitMQ... No alternative.
num = 0
if num in run_cases:
    print("\n\nCASE %i" % num)
    logfile = init_logging(num)
    try:
        connector = init_connector([node1_ok])
        wait(5)
        print("\n\tLog into RabbitMQ host\n\n\tRun\n\tservice rabbitmq-server stop\n\tAfter starting tp send messages... You have 10 seconds of sending messages.")
        send_messages(20, connector, wait=0.5)
        wait(1)
        gentle_close(connector)
    except Exception as e:
        print('Caught!')
    print("\n\tLog into RabbitMQ host\n\n\tRun\n\tservice rabbitmq-server start\n\tservice handle restart")
    lines = []
    # (1) Connecting to rabbit. This must succeed for the test to do what we want to test!
    lines.append('Opening connection to RabbitMQ...')
    lines.append('Connection to RabbitMQ at pid1.dkrz.de opened...')
    lines.append('Opening channel... done.')
    lines.append('Set confirm delivery... done.')
    lines.append('Setup is finished. Publishing may start.')
    # (2) We want the connection to be totally ready when the messages start
    lines.append('Ready to publish messages to RabbitMQ. No messages waiting yet.')
    # (3) Rabbit fails...
    lines.append('Connection to RabbitMQ was closed. Reason: Not specified.')
    # (4) Trying to reconnect...
    lines.append('Trying to reconnect to RabbitMQ in 0.5 seconds.')
    # (5) Reconnect fails...
    lines.append('Opening connection to RabbitMQ...')
    lines.append('[Errno 101] Network is unreachable.')
    # (6) At the end, the thread is joined:
    #lines.append('[MainThread]: Joining... done')
    #
    # TODO: Can not run checks, because of Exception!
    #
    if not all_lines_found(lines, logfile):
        not_pass.append(num)
    ### TODO ###


# CASE 1:
# We connect, send and finish gently.
# Between these things, we leave enough time for the tasks to finish without overlap.
num = 1
if num in run_cases:
    print("\n\nCASE %i" % num)
    logfile = init_logging(num)
    connector = init_connector([node1_ok])
    wait(10)
    send_messages(100, connector)
    wait(15)
    gentle_close(connector)
    lines = []
    # (1) When we send, the connection is already complete. No sending before that:
    lines.append('Ready to publish messages to RabbitMQ. No messages waiting yet.')
    # (2) All messages were acked:
    lines.append(['Received ack for delivery tag 100. Waiting for 0 confirms.',
                 'Received ack for delivery tag 100 and all below. Waiting for 0 confirms.'])
    # (3) When we finish, all messages were already sent/acked:
    lines.append('Gentle finish (iteration 0): No more pending')
    # (4) At the end, the thread is joined:
    lines.append('[MainThread]: Joining... done')
    if not all_lines_found(lines, logfile):
        not_pass.append(num)


# CASE 2
# We connect, send and finish gently.
# We send messages right after connecting, so messages should accumulate before the connection is ready.
# But we leave time before closing.
num = 2
if num in run_cases:
    print("\n\nCASE %i" % num)
    logfile = init_logging(num)
    connector = init_connector([node1_ok])
    send_messages(100, connector)
    wait(10)
    gentle_close(connector)
    lines = []
    # (1) When we send, the connection is not complete yet:
    lines.append('messages are already waiting to be published.')
    # (2) All messages were acked:
    lines.append(['Received ack for delivery tag 100. Waiting for 0 confirms.',
                  'Received ack for delivery tag 100 and all below. Waiting for 0 confirms.'])
    # (3) When we finish, all messages were already sent/acked:
    lines.append('Gentle finish (iteration 0): No more pending')
    # (4) At the end, the thread is joined:
    lines.append('[MainThread]: Joining... done')
    if not all_lines_found(lines, logfile):
        not_pass.append(num)

# CASE 3
# We connect, send and finish gently.
# We do all of this quickly.
num = 3
if num in run_cases:
    print("\n\nCASE %i" % num)
    logfile = init_logging(num)
    connector = init_connector([node1_ok])
    send_messages(500, connector)
    gentle_close(connector)
    lines = []
    # (1) When we send, the connection is not complete yet:
    lines.append('messages are already waiting to be published.')
    # (2) At close down, we may not have had the time to send/ack all:
    lines.append(['Gentle finish (iteration 0): Some pending messages left.',
                  'Gentle finish (iteration 0): No more pending messages.'])
    # (3) At the end, the thread is joined:
    lines.append('[MainThread]: Joining... done')
    if not all_lines_found(lines, logfile):
        not_pass.append(num)
    # This may happen before or after (2):
    # (4) All messages were acked:
    lines.append(['Received ack for delivery tag 500. Waiting for 0 confirms.',
                  'Received ack for delivery tag 500 and all below. Waiting for 0 confirms.'])


# CASE 4
# The first connection fails, the second succeeds.
num = 4
if num in run_cases:
    print("\n\nCASE %i" % num)
    logfile = init_logging(num)
    temp_node1_ok = {'user':'esgf-publisher', 'password':password_node1, 'url':'pid1.dkrz.de', 'priority':2}
    temp_node2_ok = {'user':'esgf-publisher', 'password':'foo', 'url':'pcmdi10.llnl.gov', 'priority':1}
    connector = init_connector([temp_node1_ok, temp_node2_ok])
    wait(1)
    send_messages(50, connector, wait=0.5)
    gentle_close(connector)
    lines = []
    # (1) We connect first to the node with the wrong password
    lines.append('Connecting to RabbitMQ at pcmdi10.llnl.gov...')
    lines.append('Caught Authentication Exception during connection ("ProbableAuthenticationError")')
    lines.append('Failed connection to RabbitMQ at pcmdi10.llnl.gov. Reason: ProbableAuthenticationError issued by pika.')
    # (2) Then we reconnect to the other node...
    lines.append('Connection failure: Trying to connect (now) to pid1.dkrz.de.')
    lines.append('Trying to reconnect to RabbitMQ in 0 seconds.')
    lines.append('Connecting to RabbitMQ at pid1.dkrz.de...')
    lines.append('Connection to RabbitMQ at pid1.dkrz.de opened...')
    # (3) At close down, we haven't had the time to send/ack all:
    lines.append('Gentle finish (iteration 0): Some pending messages left.')
    # (4) All messages were acked:
    lines.append(['Received ack for delivery tag 50. Waiting for 0 confirms.',
                  'Received ack for delivery tag 50 and all below. Waiting for 0 confirms.'])
    # (5) Eventually, they are all acked:
    lines.append('No more pending messages.')
    # (6) At the end, the thread is joined:
    lines.append('[MainThread]: Joining... done')
    if not all_lines_found(lines, logfile):
        not_pass.append(num)


# CASE 5
# The exchange is missing...
# FALLBACK too!
num = 5
if num in run_cases:
    print("\n\nCASE %i" % num)
    logfile = init_logging(num)
    connector = init_connector([node1_ok], 'fooexchange')
    send_messages(50, connector)
    gentle_close(connector)
    lines = []
    # (1) Trying to connect, receiving a channel close for the exchange...
    lines.append('Opening connection to RabbitMQ...')
    lines.append('Connection to RabbitMQ at pid1.dkrz.de opened...')
    lines.append("Channel was closed: NOT_FOUND - no exchange 'fooexchange' in vhost '/' (code 404)")
    lines.append('Channel closed because the exchange "fooexchange" did not exist.')
    # (2) Trying to reconnect with a different exchange...
    lines.append('Setting exchange name to fallback exchange "FALLBACK"')
    lines.append('Channel reopen: Sending all messages that have not been confirmed yet...')
    lines.append('unconfirmed messages were saved and are sent now.')
    # (3) That also does not exist...
    lines.append("Channel was closed: NOT_FOUND - no exchange 'FALLBACK' in vhost '/' (code 404)")
    # (4) So gentle-close waits in vain, while reconnecting tries and tries...
    lines.append('We have waited long enough. Now closing by force.')
    lines.append('At close down: 50 pending messages')
    # (5) At the end, the thread is joined:
    lines.append('[MainThread]: Joining... done')
    if not all_lines_found(lines, logfile):
        not_pass.append(num)


# TODO
# Missing cases
# FALLBACK exists!


#
# Finally...
#

if len(not_pass)==0:
    print('\n\nALL PASSED (tests %s)' % run_cases)
else:
    print('\n\nNot pass: %s' % not_pass)

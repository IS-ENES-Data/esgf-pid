from rabbitcredentials_SECRET import PREFIX, RABBIT_EXCHANGE
import esgfpid
import time
import datetime

today = datetime.datetime.now().strftime('%Y-%m-%d')
filename_for_logging_handles = 'handles_created_during_tests_%s.txt' % today


def init_connector(list_of_nodes, exch=RABBIT_EXCHANGE):
    print('Init connector (%s)' % list_of_nodes)
    nodemanager = esgfpid.rabbit.nodemanager.NodeManager()
    for item in list_of_nodes:
        item['username'] = item['user']
        item['host'] = item['url']
        item['exchange_name'] = exch
        if 'password' not in item:
            raise ValueError('This test can not cope with open nodes. Use the entire library for this.')
        nodemanager.add_trusted_node(**item)
    connector = esgfpid.rabbit.asynchronous.AsynchronousRabbitConnector(nodemanager)
    connector.start_rabbit_thread()
    return connector

def send_one_test_message(num, connector):
    foo = 'foo'+str(num)
    pid = connector.send_message_to_queue({'test':'test'})
    return pid

def send_messages(n, connector, wait=0):
    with open(filename_for_logging_handles, 'a') as text_file:
        print('Sending %i messages.' % n)
        for i in xrange(n):
            pid = send_one_test_message(i, connector)
            text_file.write(',%s' % pid)
            time.sleep(wait)

def gentle_close(connector):
    print('Stopping thread, waiting for pending messages...')
    connector.finish_rabbit_thread()

def force_close(connector):
    print('Stopping thread, NOT waiting for pending messages...')
    connector.force_finish_rabbit_thread()


from rabbitcredentials_SECRET import PREFIX, RABBIT_EXCHANGE
import esgfpid
import time
import datetime

today = datetime.datetime.now().strftime('%Y-%m-%d')
filename_for_logging_handles = 'handles_created_during_tests_%s.txt' % today



def init_connector(list_of_nodes, exch=RABBIT_EXCHANGE):
    print('Init connector (%s)' % list_of_nodes)
    connector = esgfpid.Connector(
        handle_prefix=PREFIX,
        messaging_service_credentials=list_of_nodes,
        messaging_service_exchange_name=exch,
        data_node='data.dkrz.foo.de',
        test_publication=True
    )
    connector.start_messaging_thread()
    return connector

def data_cart(num, connector):
    foo = 'foo'+str(num)
    pid = connector.create_data_cart_pid({foo:'hdl:123/345','bar':None,'baz':'hdl:123/678'})
    return pid
    #print('Data cart %i: hdl.handle.net/%s?noredirect' % (num, pid))

def send_one_test_message(num, connector):
    return data_cart(num, connector)

def send_messages(n, connector, wait=0):
    with open('filename_for_logging_handles', 'a') as text_file:
        print('Sending %i messages.' % n)
        for i in range(n):
            pid = send_one_test_message(i, connector)
            text_file.write(';%s' % pid)
            time.sleep(wait)

def gentle_close(connector):
    print('Stopping thread, waiting for pending messages...')
    connector.finish_messaging_thread()

def force_close(connector):
    print('Stopping thread, NOT waiting for pending messages...')
    connector.force_finish_messaging_thread()
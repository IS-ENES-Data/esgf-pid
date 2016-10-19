import esgfpid.rabbit.rabbit
import esgfpid.check
import logging
import time
import datetime

TESTVALUES_REAL = dict(
    url_messaging_service='handle-esgf.dkrz.de',
    messaging_exchange='rabbitsender_integration_tests',
    exchange_no_queue='rabbitsender_integration_tests_no_queue',
    exchange_name='rabbitsender_integration_tests', # TODO JUST ONE NAME!
    rabbit_username='sendertests',
    rabbit_password='testmebaby',
    routing_key='test_routing_key',
)

#logging.basicConfig(level=logging.DEBUG)
logdate = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
FORMAT = "%(asctime)-15s %(levelname)s:%(name)s:%(message)s"  
logging.basicConfig(level=logging.DEBUG, filename='test_log_'+logdate+'.log', filemode='w', format=FORMAT)


args = dict(
    messaging_service_username = TESTVALUES_REAL['rabbit_username'],
    messaging_service_password = TESTVALUES_REAL['rabbit_password'],
    messaging_service_urls = TESTVALUES_REAL['url_messaging_service'],
    messaging_service_exchange_name = TESTVALUES_REAL['messaging_exchange'],
    print_to_console = True,
    print_success_to_console = True
)
esgfpid.check_pid_queue_availability(**args)


if True:

    # Test variables:
    routing_key = TESTVALUES_REAL['routing_key']
    routing_key = 'BLABLA'
    num_messages = 5
    # Make connection:
    #testrabbit = self.make_rabbit_with_access()
    testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(
          #exchange_name=TESTVALUES_REAL['exchange_name'],
          exchange_name='FOOEXCHANGE',
          urls_fallback=TESTVALUES_REAL['url_messaging_service'],
          url_preferred=None,
          username=TESTVALUES_REAL['rabbit_username'],
          password=TESTVALUES_REAL['rabbit_password'],
    )
    testrabbit.start()

    # Run code to be tested:
    for i in xrange(num_messages):
          print("Publishing message %i..." % i)
          testrabbit.send_message_to_queue({"stuffee":"foo"+str(i+1),"ROUTING_KEY":routing_key})
          time.sleep(1)
          print("Publishing message %i... done." % i)


    # Close connection after some time:
    time.sleep(60*5)
    testrabbit.finish()

    # Check result:
    leftovers = testrabbit.get_leftovers()
    if not len(leftovers)==0:
        print('There is leftovers: '+str(len(leftovers)))
        for message in leftovers:
            print('Leftover: '+str(message))
    if not testrabbit.is_finished():
        'Thread is still alive.'
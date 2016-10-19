import esgfpid.rabbit.rabbit
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
logging.getLogger('esgfpid.rabbit.asynchronous.thread_builder').setLevel(logging.DEBUG)
logging.getLogger('pika').setLevel(logging.DEBUG)

# Test variables:
routing_key = TESTVALUES_REAL['routing_key']
num_messages = 150

# Make connection:
#testrabbit = self.make_rabbit_with_access()
testrabbit = esgfpid.rabbit.rabbit.RabbitMessageSender(
      exchange_name=TESTVALUES_REAL['exchange_name'],
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
      print("Publishing message %i... done." % i)
      time.sleep(3)


# Close connection after some time:
#print('Wait to close... 1')
#time.sleep(5)
print('Try to close... 1')
testrabbit.finish()

# Check result:
leftovers = testrabbit.get_leftovers()
if not len(leftovers)==0:
      print('There is leftovers: '+str(len(leftovers)))
if not testrabbit.is_finished():
      print('Thread is still alive.')

# Close connection after some time:
print('Wait to close... 2')
time.sleep(5)
print('Try to close... 2')
testrabbit.finish()

# Check result:
leftovers = testrabbit.get_leftovers()
if not len(leftovers)==0:
      print('There is leftovers: '+str(len(leftovers)))
if not testrabbit.is_finished():
      print('Thread is still alive.')


# Close connection after some time:
print('Wait to close... 2')
time.sleep(5)
print('Try to force-close... 2')
testrabbit.force_finish()
print('Force-close done')

# Check result:
leftovers = testrabbit.get_leftovers()
if not len(leftovers)==0:
      print('There is leftovers: '+str(len(leftovers)))
if not testrabbit.is_finished():
      print('Thread is still alive.')
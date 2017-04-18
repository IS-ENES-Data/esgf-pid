import unittest
import argparse
import logging
import datetime
import sys
import esgfpid.utils
import globalvar

# Setup logging:
path = 'logs'
logdate = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
filename = path+'/test_log_'+logdate+'.log'
esgfpid.utils.ensure_directory_exists(path)
logging.basicConfig(level=logging.DEBUG, filename=filename, filemode='w')
pikalogger = logging.getLogger('pika')
pikalogger.setLevel(logging.INFO)
#pikalogger.propagate = False


if __name__ == '__main__':
    desc = ('Test script for esgfpid library, including unit tests and integration '+
           'tests.')
    
    # Argument parsing
    parser = argparse.ArgumentParser(description=desc)
    
    # Unit or integration?
    parser.add_argument('-u','--unit', dest='unit',
                   help=('Run the unit tests?'),
                   action='store_true')
    parser.add_argument('-i','--integration', dest='inte',
                   help=('Run the integration tests?'),
                   action='store_true')
    parser.set_defaults(unit=False)
    parser.set_defaults(inte=False)

    # Modules to be tested:
    parser.add_argument('-m','--modules', metavar='mod', nargs='*',
                   help=('Which modules to test. '+
                         'Possible values: "all", "solr", "rabbit", "publish", "unpublish", '+
                         '"errata", "utils", "api", "check", "messages", "consistency", "data_cart", '+
                         '"nodemanager").'
                         'Defaults to "all".'),
                   default=['all'], action='store')

    # Synchron or asynchron?
    parser.add_argument('-noa','--no-asynchron', dest='asyn',
                   help=('Omit the asynchronous rabbit tests?'),
                   action='store_false')
    parser.add_argument('-nos','--no-synchron', dest='syn',
                   help=('Omit the synchronous rabbit tests?'),
                   action='store_false')
    parser.set_defaults(asyn=True)
    parser.set_defaults(syn=True)
    # Slow?
    parser.add_argument('-ls','--leave_out_slow', dest='leave_out',
                   help=('Leave out the slow tests?'),
                   action='store_true')
    parser.set_defaults(slow=False)
    # Open nodes?
    parser.add_argument('-open','--allow_open', dest='open_allowed',
                   help=('Can the library cope with open nodes?'),
                   action='store_true')
    parser.set_defaults(open_allowed=False)

    # Parse the args:
    param = parser.parse_args()
    print('Modules to be tested: '+ ', '.join(param.modules))

    # Inform user about skipping slow tests...
    if param.leave_out == True:
        globalvar.QUICK_ONLY = True
        print('Leaving out the slow tests (%s).' % globalvar.QUICK_ONLY)
    else:
        globalvar.QUICK_ONLY = False
    if param.open_allowed == True:
        globalvar.RABBIT_OPEN_NOT_ALLOWED = False
        print('Running tests for open RabbitMQ nodes, too (%s).' % globalvar.QUICK_ONLY)
    else:
        globalvar.RABBIT_OPEN_NOT_ALLOWED = True


    # Collect tests:
    verbosity = 5
    descriptions = 0
    print '\nCollecting tests:'
    tests_to_run = []
    numtests = 0

    if param.unit:
    
        if 'utils' in param.modules or 'all' in param.modules:

            from testcases.util_tests import UtilsTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(UtilsTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.util_logging_tests import UtilsLoggingTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(UtilsLoggingTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.errormessage_util_tests import ErrorMessageUtilsTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(ErrorMessageUtilsTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'messages' in param.modules or 'all' in param.modules:

            from testcases.messages_tests import MessageCreationTestcase
            tests = unittest.TestLoader().loadTestsFromTestCase(MessageCreationTestcase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'check' in param.modules or 'all' in param.modules:

            from testcases.check_tests import CheckTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(CheckTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'data_cart' in param.modules or 'all' in param.modules:

            from testcases.data_cart_tests import DataCartTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(DataCartTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'solr' in param.modules or 'all' in param.modules:

            from testcases.solr.solr_utils_tests import SolrUtilsTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrUtilsTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.solr.solr_tests import SolrTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.solr.solr_task1_tests import SolrTask1TestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrTask1TestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.solr.solr_task2_tests import SolrTask2TestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrTask2TestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.solr.solr_server_tests import SolrServerConnectorTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrServerConnectorTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'nodemanager' in param.modules or 'rabbit' in param.modules or 'all' in param.modules:

            from testcases.rabbit.nodemanager_tests import NodemanagerTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(NodemanagerTestCase)
            tests_to_run.append(tests)
            numtests += tests.countTestCases()

        if 'rabbit' in param.modules or 'all' in param.modules:

            from testcases.rabbit.rabbit_api_tests import RabbitTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(RabbitTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n
            
            from testcases.rabbit.rabbitutil_tests import RabbitUtilsTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(RabbitUtilsTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            if param.syn:

                from testcases.rabbit.syn.rabbit_synchronous_tests import RabbitConnectorTestCase
                tests = unittest.TestLoader().loadTestsFromTestCase(RabbitConnectorTestCase)
                tests_to_run.append(tests)
                n = tests.countTestCases()
                numtests += n

            if param.asyn:

                # This tests the API and the thread
                from testcases.rabbit.asyn.rabbit_asynchronous_tests import RabbitAsynConnectorTestCase
                tests = unittest.TestLoader().loadTestsFromTestCase(RabbitAsynConnectorTestCase)
                tests_to_run.append(tests)
                numtests += tests.countTestCases()

                # Confirmer is pretty isolated and easy to test.
                from testcases.rabbit.asyn.thread_confirmer_tests import ThreadConfirmerTestCase
                tests = unittest.TestLoader().loadTestsFromTestCase(ThreadConfirmerTestCase)
                tests_to_run.append(tests)
                numtests += tests.countTestCases()

                # Feeder needs some mocking...
                from testcases.rabbit.asyn.thread_feeder_tests import ThreadFeederTestCase
                tests = unittest.TestLoader().loadTestsFromTestCase(ThreadFeederTestCase)
                tests_to_run.append(tests)
                numtests += tests.countTestCases()

                # Returner needs some mocking...
                from testcases.rabbit.asyn.thread_returner_tests import ThreadReturnerTestCase
                tests = unittest.TestLoader().loadTestsFromTestCase(ThreadReturnerTestCase)
                tests_to_run.append(tests)
                numtests += tests.countTestCases()
            
                # Shutter needs some mocking...
                from testcases.rabbit.asyn.thread_shutter_tests import ThreadShutterTestCase
                tests = unittest.TestLoader().loadTestsFromTestCase(ThreadShutterTestCase)
                tests_to_run.append(tests)
                numtests += tests.countTestCases()

                # Builder needs lots of mocking
                from testcases.rabbit.asyn.thread_builder_tests import ThreadBuilderTestCase
                tests = unittest.TestLoader().loadTestsFromTestCase(ThreadBuilderTestCase)
                tests_to_run.append(tests)
                numtests += tests.countTestCases()

        if 'errata' in param.modules or 'all' in param.modules:

            from testcases.errata_tests import ErrataTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(ErrataTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'unpublish' in param.modules or 'all' in param.modules:

            from testcases.unpublication_tests import UnpublicationTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(UnpublicationTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'publish' in param.modules or 'all' in param.modules:

            from testcases.publish_tests import PublishTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(PublishTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'api' in param.modules or 'all' in param.modules:

            from testcases.connector_tests import ConnectorTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(ConnectorTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'consistency' in param.modules or 'all' in param.modules:

            from testcases.consistency_tests import ConsistencyTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(ConsistencyTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

    if param.inte:

        if 'rabbit' in param.modules or 'all' in param.modules:

            if param.asyn:
                from testcases.integration_rabbit_tests_asynchron import RabbitIntegrationTestCase
            else:
                from testcases.integration_rabbit_tests_synchron import RabbitIntegrationTestCase

            tests = unittest.TestLoader().loadTestsFromTestCase(RabbitIntegrationTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'solr' in param.modules or 'all' in param.modules:

            from testcases.integration_solr_tests import SolrIntegrationTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrIntegrationTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.integration_solr_server_tests import SolrServerIntegrationTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrServerIntegrationTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.integration_solr_task1_tests import SolrTask1IntegrationTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrTask1IntegrationTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

    # Run all of them:
    print 'Run '+str(numtests)+' tests.'
    test_suites = unittest.TestSuite(tests_to_run)
    print '\nStarting tests:'
    logging.info("Starting tests:")
    test_result = unittest.TextTestRunner(descriptions=descriptions, verbosity=verbosity).run(test_suites)
    sys.exit(not test_result.wasSuccessful())

    # Run with:
    # python -m coverage run main_test_script.py
    # python -m coverage html

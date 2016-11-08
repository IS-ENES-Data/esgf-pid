import unittest
import argparse
import logging
import datetime
import os
import sys
sys.path.append("..")
import esgfpid

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
                         '"errata", "utils", "api", "check", "messages", "consistency", "shopping_cart"). '+
                         'Defaults to "all".'),
                   default=['all'], action='store')

    # Synchron or asynchron?
    parser.add_argument('-a','--asynchron', dest='asyn',
                   help=('Run the asynchronous rabbit tests?'),
                   action='store_true')
    parser.add_argument('-s','--synchron', dest='asyn',
                   help=('Run the synchronous rabbit tests?'),
                   action='store_false')
    parser.set_defaults(asyn=True)

    param = parser.parse_args()
    #print('Specified test types: '+str(param.testtype))
    print('Modules to be tested: '+ ', '.join(param.modules))

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

            from testcases.rabbit_rabbitutil_tests import RabbitUtilsTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(RabbitUtilsTestCase)
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

            pass
            # TODO
            #from testcases.check_tests import CheckTestCase
            #tests = unittest.TestLoader().loadTestsFromTestCase(CheckTestCase)
            #tests_to_run.append(tests)
            #n = tests.countTestCases()
            #numtests += n

        if 'shopping_cart' in param.modules or 'all' in param.modules:

            from testcases.shopping_cart_tests import ShoppingCartTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(ShoppingCartTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'solr' in param.modules or 'all' in param.modules:

            from testcases.solr_utils_tests import SolrUtilsTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrUtilsTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.solr_tests import SolrTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.solr_task1_tests import SolrTask1TestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrTask1TestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.solr_task2_tests import SolrTask2TestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrTask2TestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

            from testcases.solr_server_tests import SolrServerConnectorTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(SolrServerConnectorTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'rabbit' in param.modules or 'all' in param.modules:

            if param.asyn:

                # TODO
                
                #from testcases.rabbit_rabbit_asyn_tests import RabbitTestCase
                #tests = unittest.TestLoader().loadTestsFromTestCase(RabbitTestCase)
                #tests_to_run.append(tests)
                #numtests += tests.countTestCases()

                #from testcases.rabbit_asynchronous_asynchronous_tests import RabbitAsynConnectorTestCase
                #tests = unittest.TestLoader().loadTestsFromTestCase(RabbitAsynConnectorTestCase)
                #tests_to_run.append(tests)
                #numtests += tests.countTestCases()

                #from testcases.rabbit_thread_confirmer_tests import ThreadConfirmerTestCase
                #tests = unittest.TestLoader().loadTestsFromTestCase(ThreadConfirmerTestCase)
                #tests_to_run.append(tests)
                #numtests += tests.countTestCases()

                #from testcases.rabbit_thread_acceptor_tests import ThreadAcceptorTestCase
                #tests = unittest.TestLoader().loadTestsFromTestCase(ThreadAcceptorTestCase)
                #tests_to_run.append(tests)
                #numtests += tests.countTestCases()

                #from testcases.rabbit_thread_shutter_tests import ThreadShutterTestCase
                #tests = unittest.TestLoader().loadTestsFromTestCase(ThreadShutterTestCase)
                #tests_to_run.append(tests)
                #numtests += tests.countTestCases()

                #from testcases.rabbit_thread_feeder_tests import ThreadFeederTestCase
                #tests = unittest.TestLoader().loadTestsFromTestCase(ThreadFeederTestCase)
                #tests_to_run.append(tests)
                #numtests += tests.countTestCases()

                #from testcases.rabbit_thread_builder_tests import ThreadBuilderTestCase
                #tests = unittest.TestLoader().loadTestsFromTestCase(ThreadBuilderTestCase)
                #tests_to_run.append(tests)
                #numtests += tests.countTestCases()

                from testcases.rabbit_asynchronous_asynchronous_module_tests import RabbitAsynModuleTestCase
                tests = unittest.TestLoader().loadTestsFromTestCase(RabbitAsynModuleTestCase)
                tests_to_run.append(tests)
                numtests += tests.countTestCases()

            else:

                from testcases.rabbit_rabbit_syn_tests import RabbitTestCase
                tests = unittest.TestLoader().loadTestsFromTestCase(RabbitTestCase)
                tests_to_run.append(tests)
                n = tests.countTestCases()
                numtests += n

                from testcases.rabbit_synchronous_tests import RabbitConnectorTestCase
                tests = unittest.TestLoader().loadTestsFromTestCase(RabbitConnectorTestCase)
                tests_to_run.append(tests)
                n = tests.countTestCases()
                numtests += n

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

            from testcases.api_tests import ApiTestCase
            tests = unittest.TestLoader().loadTestsFromTestCase(ApiTestCase)
            tests_to_run.append(tests)
            n = tests.countTestCases()
            numtests += n

        if 'consistency' in param.modules or 'all' in param.modules:

            pass
            # TODO
            #from testcases.consistency_tests import ConsistencyTestCase
            #tests = unittest.TestLoader().loadTestsFromTestCase(ConsistencyTestCase)
            #tests_to_run.append(tests)
            #n = tests.countTestCases()
            #numtests += n

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
    # python -m coverage report

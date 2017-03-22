import logging
import requests
import sys
import json
import esgfpid.utils
import esgfpid.exceptions
import esgfpid.defaults
from esgfpid.utils import loginfo, logdebug, logtrace, logerror, logwarn

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

'''
This class is only responsible for sending queries
(that were assembled elsewhere) to a solr instance,
validate and parse the result and return it.

It basically only provides one method,
:func:`~solr.SolrServerConnector.send_query`.

'''
class SolrServerConnector(object):

    '''
    Create an instance.

    :param solr_url: Mandatory.
    :param https_verify: Mandatory. Boolean.
    :param disable_insecure_request_warning: Mandatory. Boolean.
    '''
    def __init__(self, **args):
        self.__check_presence_of_args(args)
        self.__set_attributes(args)
        self.__disable_warning_if_desired(args)

    def __check_presence_of_args(self, args):
        mandatory_args = ['solr_url', 'https_verify', 'disable_insecure_request_warning']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)

    def __set_attributes(self, args):
        self.__solr_url = args['solr_url'].strip('/')
        self.__https_verify = args['https_verify']
        self.__solr_headers = {'Accept': 'application/json,text/json', 'Content-Type': 'application/json'}

    def __disable_warning_if_desired(self, args):
        if args['disable_insecure_request_warning']:
            self.__disable_insecure_request_warning()

    def __disable_insecure_request_warning(self):
        logdebug(LOGGER, 'Disabling insecure request warnings...')
        # Source: http://stackoverflow.com/questions/27981545/suppress-insecurerequestwarning-unverified-https-request-is-being-made-in-pytho#28002687
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    '''
    Send a message to a solr instance.

    This raises SolrError exceptions in various situations:
     - Connection problems
     - Response is empty, None, or invalid JSON
     - Response returns 404 or any other HTTP code other than 200

     :raises: esgfpid.exceptions.SolrError
     :return: A JSON response. It never returns None.
    '''
    def send_query(self, query):
        response = self.__get_request_to_solr(query)
        self.__check_response_for_error_codes(response)
        response_json = self.__get_json_from_response(response)
        return response_json

    def __get_request_to_solr(self, query_dict):
        logdebug(LOGGER, 'Sending GET request to solr at "'+self.__solr_url+'".')
        try:
            resp = requests.get(
                self.__solr_url,
                params=query_dict,
                headers=self.__solr_headers,
                verify=self.__https_verify
            )
            return resp
        except requests.exceptions.ConnectionError as e:
            msg = 'Could not connect to solr: ConnectionError.'
            LOGGER.exception(msg)
            raise esgfpid.exceptions.SolrError(msg)

    def __get_json_from_response(self, response):
        try:
            response_json = json.loads(response.content)
            logdebug(LOGGER, 'Solr response ok, returned JSON content.')
            return response_json

        except (ValueError, TypeError) as e:
            msg = 'Error while parsing Solr response. It seems to be no valid JSON. Message: '+e.message
            logerror(LOGGER, msg)
            raise esgfpid.exceptions.SolrError(msg)

    def __check_response_for_error_codes(self, response):

        if response is None:
            msg = 'Solr returned no response (None)'
            logerror(LOGGER, msg)
            raise esgfpid.exceptions.SolrError(msg)

        elif response.status_code == 200:
            if response.content is None:
                msg = 'Solr returned an empty response (with content None)'
                logerror(LOGGER, msg)
                raise esgfpid.exceptions.SolrError(msg)

        elif response.status_code == 404:
            msg = 'Solr returned no response (HTTP 404)'
            logerror(LOGGER, msg)
            raise esgfpid.exceptions.SolrError(msg)

        else:
            msg = 'Solr replied with code '+str(response.status_code)
            logerror(LOGGER, msg)
            raise esgfpid.exceptions.SolrError(msg)



import logging
import requests
import sys
import json
import esgfpid.utils
import esgfpid.exceptions
import esgfpid.defaults

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

class SolrServerConnector(object):
    '''
    The SolrServerConnector only provides one method, send_query(query).

    It raises SolrError exceptions in various situations:
     - Connection problems
     - Response is empty, None, or invalid JSON
     - Response returns 404 or any other HTTP code other than 200

    It returns a JSON response. It never returns None.
    '''

    def __init__(self, **args):
        self.__check_presence_of_args(args)
        self.__set_attributes(args)
        self.__disable_warning_if_desired(args)

    def __check_presence_of_args(self, args):
        mandatory_args = ['solr_url']
        optional_args = ['https_verify', 'disable_insecure_request_warning']
        esgfpid.utils.check_presence_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.check_noneness_of_mandatory_args(args, mandatory_args)
        esgfpid.utils.add_missing_optional_args_with_value_none(args, optional_args)

    def __set_attributes(self, args):
        self.__solr_url = args['solr_url'].strip('/')
        self.__https_verify = args['https_verify'] or esgfpid.defaults.SOLR_HTTPS_VERIFY_DEFAULT
        self.__solr_headers = {'Accept': 'application/json,text/json', 'Content-Type': 'application/json'}

    def __disable_warning_if_desired(self, args):
        if args['disable_insecure_request_warning']:
            self.__disable_insecure_request_warning()

    def __disable_insecure_request_warning(self):
        LOGGER.debug('Disabling insecure request warnings...')
        # Source: http://stackoverflow.com/questions/27981545/suppress-insecurerequestwarning-unverified-https-request-is-being-made-in-pytho#28002687
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    def send_query(self, query):
        response = self.__get_request_to_solr(query)
        self.__check_response_for_error_codes(response)
        response_json = self.__get_json_from_response(response)
        return response_json

    def __get_request_to_solr(self, query_dict):
        LOGGER.debug('Sending GET request to solr at "'+self.__solr_url+'".')
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
            LOGGER.debug('Solr response ok, returned JSON content.')
            return response_json

        except (ValueError, TypeError) as e:
            msg = 'Error while parsing Solr response. It seems to be no valid JSON. Message: '+e.message
            LOGGER.error(msg)
            raise esgfpid.exceptions.SolrError(msg)

    def __check_response_for_error_codes(self, response):

        if response is None:
            msg = 'Solr returned no response (None)'
            LOGGER.error(msg)
            raise esgfpid.exceptions.SolrError(msg)

        elif response.status_code == 200:
            if response.content is None:
                msg = 'Solr returned an empty response (with content None)'
                LOGGER.error(msg)
                raise esgfpid.exceptions.SolrError(msg)

        elif response.status_code == 404:
            msg = 'Solr returned no response (HTTP 404)'
            LOGGER.error(msg)
            raise esgfpid.exceptions.SolrError(msg)

        else:
            msg = 'Solr replied with code '+str(response.status_code)
            LOGGER.error(msg)
            raise esgfpid.exceptions.SolrError(msg)


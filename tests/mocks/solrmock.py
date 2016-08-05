import json
import mock

class MockSolrInteractor(object):

    def __init__(self, solr_url='mock_solr_url', prefix='mock_prefix', https_verify=True, previous_files=None, raise_error=False):
        self.solr_url = solr_url
        self.prefix = prefix
        self.https_verify = https_verify
        self.previous_files = previous_files
        if previous_files is None:
            self.previous_files = []
        if previous_files == 'NONE':
            self.previous_files = None
        self.datasethandles_or_versionnumbers_of_allversions = dict(dataset_handles=None, version_numbers=None)
        self.raise_error = raise_error

    def retrieve_file_handles_of_same_dataset(self, *args, **kwargs):
        if self.raise_error != False:
            raise self.raise_error
        else:
            return self.previous_files

    def has_access(self):
        return True

    def retrieve_datasethandles_or_versionnumbers_of_allversions(self, drs_id):
        if self.raise_error != False:
            raise self.raise_error
        else:
            return self.datasethandles_or_versionnumbers_of_allversions

    def is_switched_off(self):
        return False
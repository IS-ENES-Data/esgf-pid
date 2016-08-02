import json
import copy

path = 'resources/default_values.json'
all_default_values = json.load(open(path))


class TestcaseData(object):
    def __init__(self, testcasename):

        self.testcasename = testcasename

        # Will be filled by user:
        self.dataset = DatasetData()
        self.file1 = FileData()
        self.file2 = FileData()

        # Message identifier
        # Together with the test case identifier, these will
        # be used to compare with the correct outcomes!
        self.dataset.testmessage_identifier = 'dataset1'
        self.file1.testmessage_identifier = 'file1'
        self.file2.testmessage_identifier = 'file2'

        # Taken from default, if not set:
        self.prefix     = str(all_default_values['defaults_connector']['prefix'])
        self.data_node  = str(all_default_values['defaults_connector']['data_node'])
        self.prefix     = str(all_default_values['defaults_connector']['prefix'])
        self.solr_url   = str(all_default_values['defaults_connector']['solr_url'])
        self.thredds_service_path  = str(all_default_values['defaults_connector']['thredds_service_path'])
        self.url_messaging_service = str(all_default_values['defaults_connector']['url_messaging_service'])
        self.messaging_exchange    = str(all_default_values['defaults_connector']['messaging_exchange'])
        self.rabbit_username       = str(all_default_values['defaults_connector']['rabbit_username'])
        self.rabbit_password       = str(all_default_values['defaults_connector']['rabbit_password'])

    def connector_args(self):
        args = dict(
            data_node=self.data_node,
            prefix=self.prefix,
            solr_url=self.solr_url,
            thredds_service_path=self.thredds_service_path,
            url_messaging_service=self.url_messaging_service,
            messaging_exchange=self.messaging_exchange,
            rabbit_username=self.rabbit_username,
            rabbit_password=self.rabbit_password
        )
        return args

    def dataset_args(self):
        self.dataset.testcase_identifier = self.testcasename
        return self.dataset._args()

    def file1_args(self):
        self.file1.testcase_identifier = self.testcasename
        return self.file1._args()

    def file2_args(self):
        self.file2.testcase_identifier = self.testcasename
        return self.file2._args()

    def prettyprint(self, short=True):
        prettystring = '*'*30
        prettystring += '***' + self.description
        prettystring = '*'*30
        if short:
            pass
        else:
            pass
        return prettystring


class DatasetData(object):
    def __init__(self, comment=None):

        # From parent:
        self.testcase_identifier = 'foo'
        self.testmessage_identifier = None

        # Can be set:
        self.comment = comment

        # Has to be set:
        self.drs_id = None
        self.version_number = None

    def _args(self):
        args = dict(
            drs_id=self.drs_id,
            version_number=self.version_number,
            testcase_identifier=self.testcase_identifier,
            testmessage_identifier=self.testmessage_identifier
        )
        if self.comment is not None:
            args['comment'] = self.comment
        return args


class FileData(object):
    def __init__(self, comment=None):

        # From parent:
        self.testcase_identifier = 'bar'
        self.testmessage_identifier = None

        # Can be set:
        self.comment = comment

        # Has to be set:
        self.filename = None
        self.handle = None

        # Taken from default, if not set:
        self.file_version  = str(all_default_values['defaults_files']['file_version'])
        self.checksum      = str(all_default_values['defaults_files']['checksum'])
        self.checksum_type = str(all_default_values['defaults_files']['checksum_type'])
        self.file_size     = str(all_default_values['defaults_files']['file_size'])
        self.publish_path  = str(all_default_values['defaults_files']['publish_path'])

    def get_entire_handle(self):
        handle = str(all_default_values['defaults_connector']['prefix'])+'/esgf_testfile_'+str(self.filename)+'_'+str(self.handle)
        if self.file_version is not None:
            handle += str(self.file_version)
        return handle

    def _args(self):
        if self.filename is None:
            raise ValueError("Filename missing!")
        if self.handle is None:
            raise ValueError("Handle missing!")
        if self.testmessage_identifier is None:
            raise ValueError("Test Message Identifier missing!")

        args = dict(
            file_name=self.filename,
            file_handle=self.get_entire_handle(),
            checksum=self.checksum,
            checksum_type=self.checksum_type,
            file_size=self.file_size,
            publish_path=self.publish_path+'/'+self.filename,
            testcase_identifier=self.testcase_identifier,
            testmessage_identifier=self.testmessage_identifier
        )

        if self.file_version is not None:
            args['file_version'] = self.file_version
        if self.comment is not None:
            args['comment'] = self.comment
        return args


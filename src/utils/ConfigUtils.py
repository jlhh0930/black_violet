from configparser import ConfigParser
from pathlib import Path

import yaml

#section names map to the sections in the config.ini files
section_names = 'default', 'aws', 'redshift', 's3', 'sqs', 'hst', 'sftp'

class Config:
    def __init__(self):
        #define all the attributes / properties
        self.env = None
        self.loading_type = None
        self.job = None
        self.project_root = Path(__file__).parent.parent.parent
        self.publish = None
        self.pipeline = None
        self.extract = None
        self.start_date = None
        self.end_date = None
        self.distributors = []
        self.states = []
        self.extracts = []
        self.extract_details = {}
        self.sftp_kms = None

    def load_config(self):
        parser = ConfigParser()
        parser.optionxform = str
        found = parser.read('{root}/config/config_{env}.ini'.format(root=self.project_root, env=self.env))
        if not found:
            raise ValueError('No config file found for {env}'.format(env=self.env))
        for name in section_names:
            self.__dict__.update(parser.items(name))

    # For config values that are frequently passed around but NOT contained in the config.ini file,
    # set individually
    def set_env(self, environment):
        self.__dict__['env'] = environment

    def set_job(self, job):
        self.__dict__['job'] = job

    def set_publish(self, publish):
        self.__dict__['publish'] = publish

    def set_extract(self, extract):
        self.__dict__['extract'] = extract

    def set_start_date(self, start_date):
        self.__dict__['start_date'] = start_date

    def set_end_date(self, end_date):
        self.__dict__['end_date'] = end_date

    def set_pipeline(self, pipeline):
        self.__dict__['pipeline'] = pipeline

    # For values set in a yaml file
    def set_extracts(self):
        self.__dict__['extracts'] = list(self.__dict__['extract_details']['default']['extracts'].keys())

    def set_sftp_kms(self):
        self.__dict__['sftp_kms'] = list(self.__dict__['extract_details']['kms'].keys())

    def set_states(self):
        self.__dict__['states'] = list(self.__dict__['extract_details']['states'].keys())

    def set_states(self):
        self.__dict__['clients'] = list(self.__dict__['extract_details']['clients'].keys())

    # For dicts set in an env-specific yaml file
    def get_extract_details(self):
        with open('{root}/config/extract_details_{env}.yaml'.format(root=self.project_root, env=self.env), 'r') as file:
            yaml_data = yaml.safe_load(file)
            self.set_extracts()
            self.set_sftp_kms()
            self.set_states()
            self.set_clients()
            self.__dict__['extract_details'] = yaml_data

config: Config = Config()
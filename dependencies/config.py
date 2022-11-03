from configparser import ConfigParser

class Config:

    def __init__(self, config_file):
        global conf_read
        conf_read = ConfigParser()
        conf_read.read(config_file)

    def _get_app_config_value(self, section, tag):
        return conf_read.get(section, tag).strip()

    def get_application_name(self):
        return self._get_app_config_value('SPARK', 'appName')

    def get_master(self):
        return self._get_app_config_value('SPARK', 'master')

    def get_name_database(self):
        return self._get_app_config_value('DATABASE', 'database')

    def get_host_user(self):
        return self._get_app_config_value('DATABASE', 'username')

    def get_host_password(self):
        return self._get_app_config_value('DATABASE', 'password')

    def get_list_tables(self):
        return self._get_app_config_value('DATABASE', 'tables')

    def get_server(self):
        return self._get_app_config_value('DATABASE', 'server')

    def get_path_hdfs_output(self):
        return self._get_app_config_value('HDFS', 'path_output')
import logging
import os
import yaml


class ProgramConfiguration(object):
    """
    Class used to handle and maintain all parameters of this program (timeouts, some other values...)
    """
    _config_tech = None
    _config_func = None
    _logger = logging.getLogger()

    def __init__(self, config_file_tech_path, config_file_func_path):
        """
        Constructor - Loads the given external JSON configuration file. Raises an error if not able to do it.
        :param config_file_tech_path: (string) full path to the technical YAML configuration file
        :param config_file_func_path: (string) full path to the functional YAML configuration file
        """

        # Technical configurations
        if os.path.exists(config_file_tech_path):
            with open(config_file_tech_path, 'rt') as f:
                self._config_tech = yaml.load(f)
                self._logger.info(
                    "Technical YAML configuration file ('{}') successfully loaded".format(config_file_tech_path))
        else:
            raise Exception("Could not load the technical YAML configuration file '{}'".format(config_file_tech_path))

        # Functional configurations
        if os.path.exists(config_file_func_path):

            with open(config_file_func_path, 'rt') as f:
                self._config_func = yaml.load(f)
                self._logger.info(
                    "Functional YAML configuration file ('{}') successfully loaded".format(config_file_tech_path))
        else:
            raise Exception("Could not load the functional YAML configuration file '{}'".format(config_file_tech_path))

    # ------------------------Technical configurations-----------------------------------------

    def get_bucket_raw(self):
        """
        Retrieves the name of the bucket where raw data is located.
        :return: the name of the bucket where raw data is located (str).
        """
        return self._config_tech['env']['bucket_raw']

    def get_bucket_clean(self):
        """
        Retrieves the name of the bucket where to put clean data.
        :return: the name of the bucket where to put clean data(str).
        """
        return self._config_tech['env']['bucket_clean']

    def get_aws_sync_filter_time(self):
        """
        Retrieves the time filter for data synchronisation.
        !!! It is mandatory!!!
        :return: the number of last months to consider for the
        synchronisation when we deal with big tables (int).
        """
        return self._config_tech['env']['aws_sync_filter_time']

    def get_spark_conf(self):
        """
        Retrieves Spark configurations list.
        :return: a list of tuples <spark_configuration_name, value>
        for spark configuration (list).
        """
        return self._config_tech['spark_conf']

    def get_datasource_conf(self, src):
        """
        Retrieves DataSource Input Output Conf
        :param src: the data source to process
        :return: a dict contains input paths for csv files and output tables
        """
        return self._config_tech['data_src'][src]

    # ------------------------Functional configurations------------------------------------------


    def get_scope_datalake_tables(self, scope):
        """
        Retrieves the scope (the perimeter) of tables to process.
        :param scope: the scope of tables (str).
        :return: a dictionary {table_schema: list_of_tables_names}(dict).
        """
        return self._config_func['scope'][scope]['datalake']

    def get_columns_to_drop(self):
        """
        Retrieves a dictionary of columns to drop for specific tables.
        :return: a dictionary {table_name: list_of_columns_to_drop}(dict).
        """
        ret = self._config_func['columns_to_drop'] if "columns_to_drop" in self._config_func.keys() else {}
        return ret

    def get_filter_time(self, scope):
        """
        Retrieves the integer time filter if it exists,
        otherwise return a default string value ("Full").
        :param scope: the scope of tables (str).
        :return: an integer or a string (int or str).
        """
        try:
            time_delta = self._config_func['scope'][scope]['filter_time']
        except Exception as e:
            time_delta = 'Full'

        return time_delta

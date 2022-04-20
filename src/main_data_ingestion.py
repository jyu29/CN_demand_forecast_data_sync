# -*- coding: utf-8 -*-
import logging

import src.tools.constants as cst

from src.config import config
from src.config.pgconf import ProgramConfiguration
from src.tools import tools as ut

from pyspark.sql import SparkSession,SQLContext
from pyspark.conf import SparkConf


from time import gmtime, strftime
import boto3
import os

from src.preparation import data_ingestion

logger = logging.getLogger('Data Ingestion')

# ---------------Global variables that will be initialized in the __main__
# Spark session.
spark = None

# Dictionary used to remove some unwanted columns from some tables.
columns_filter_dict = {}


if __name__ == '__main__':
    # --------------- Handling mandatory arguments -----------------


    # Parsing and getting the logger.
    args = config.basic_parse_args()
    technical_config_file = vars(args)['config_file']
    log_config_file = vars(args)['log_file']

    # Getting the name of the functional configuration file.
    functional_config_file = cst.FUNCTIONAL_CONF_FILE

    # Getting the scope of the data we need to process.
    scope = vars(args)['scope']

    # Getting the process of the script
    process = vars(args)['process']

    # Configuring the whole program (logging, external config files, singletons, ...).
    config.configure_logging(log_config_file)
    config.pgconf = ProgramConfiguration(technical_config_file, functional_config_file)
    # Bucket's name for raw data.
    bucket_raw_fcst = config.pgconf.get_bucket_raw()
    # Bucket's name for clean data.
    bucket_clean_fcst = config.pgconf.get_bucket_clean()

    # The dictionary {table_name:list_of_columns_to_drop}.
    columns_filter_dict = config.pgconf.get_columns_to_drop()
    # The dictionary {schema:list_of_tables_name}.
    datalake_schemas_and_tables_dict = config.pgconf.get_scope_datalake_tables(scope)
    # The number of latest months to process. It can be either an integer
    # or a string (time_filter='Full' when 'filter_time' is not found inside the
    # 'scope' of the 'conf/functional.yml' file. In this case, all months are processed).
    time_filter = config.pgconf.get_filter_time(scope)
    # The s3 root path URL.
    s3_root_path = cst.URL_S3

    logger.info('---> Setting up the spark session...')
    # Setting up Spark configurations.
    conf = SparkConf()
    spark_conf_list = []
    # Defining Spark configurations and adding it to the list
    # of configurations.
    for conf_param, conf_value in config.pgconf.get_spark_conf().items():
        spark_conf_list.append((conf_param, str(conf_value)))
    logger.info('---> Spark configurations... \n')
    conf = conf.setAll(spark_conf_list)
    logger.info('\n Spark Connection ...')
    # Setting up Spark connection.
    spark = SparkSession.builder. \
        config(conf=conf). \
        enableHiveSupport(). \
        getOrCreate()
    # Output only Spark's ERROR.
    spark.sparkContext.setLogLevel("ERROR")

    logger.info('---> Setting up the s3 session and connecting to the s3 client...')
    # Retrieving aws credentials from environment variables.
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    # Retrieving aws region.
    region_name = cst.REGION_S3
    # Setting up boto3 connection.
    session = boto3.session.Session(region_name=region_name)
    s3client = session.client('s3',
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)

    # =============================================================================================
    #
    # =============================================================================================
    logger.info("===> The process starts at {} \n".format(strftime(" %Hh%Mmin, %d-%m-%Y", gmtime())))

    sub_title = "\n***  Getting information about tables' headers directory (paths etc) ***"
    logger.info("{:<32}".format(sub_title) + '\n')

    if process == 'ingestion' or process == 'full':

        # Extracting here the list of all paths(sub-folders and csv files) inside the tables' headers directory
        # (Ex: s3a://fcs-raw/TABLE_COLUMN) as well as  the most recent timestamped sub-folder of this
        # directory(Ex: s3a://fcs-raw/TABLE_COLUMN/202009).
        headers_paths_list, headers_last_sub_folder = ut.get_timestamped_headers_folder(bucket_name=bucket_raw_fcst,
                                                                                        s3client=s3client)
        logger.info('\n' + 90 * '-')

        # For all schemas, apply the data ingestion process to all tables.
        for key, value in datalake_schemas_and_tables_dict.items():
            # For a given schema, if the list of tables does exist and is not empty then apply the data ingestion process.
            if (value is not None) and len(value) > 0:
                data_ingestion.main_pipe(bucket_raw_data=bucket_raw_fcst, table_source='datalake', bucket_schema=key, list_table_schema=value,
                                         bucket_cleaned_data=bucket_clean_fcst, time_filter=time_filter,
                                         spark_session=spark, columns_filter_dict=columns_filter_dict,
                                         headers_paths_list=headers_paths_list,
                                         headers_last_sub_folder=headers_last_sub_folder,
                                         s3client=s3client, pgconf=config.pgconf)
            else:
                logger.info('***** There is no table to process in the the schema [{}] *****'.format(key))

    # Releasing connections ...
    logger.info('---> Disconnecting the spark session...')
    # closing the Spark's session.
    spark.stop()
    spark = None

    logger.info("===> The process ends at {} \n".format(strftime(" %Hh%Mmin, %d-%m-%Y", gmtime())))

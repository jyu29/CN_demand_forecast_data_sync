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
from src.preparation import data_apo

logger = logging.getLogger('Data Refining APO')

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

    #Local config
    #technical_config_file = "./conf/dev.yml"
    #log_config_file = "./conf/loggin-console.json"
    #scope = "scope_apo"

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
    schemas_and_tables_dict = config.pgconf.get_scope_tables(scope)
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

    data_apo.pipe_cleaning_apo_eu(spark_session=spark, s3client=s3client, bucket_clean=bucket_clean_fcst, time_filter=time_filter)

    # Releasing connections ...
    logger.info('---> Disconnecting the spark session...')
    # closing the Spark's session.
    spark.stop()
    spark = None

    logger.info("===> The process ends at {} \n".format(strftime(" %Hh%Mmin, %d-%m-%Y", gmtime())))

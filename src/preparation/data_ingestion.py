import itertools
import logging
from src.tools import tools as ut
from src.tools import constants as cst
from pyspark.sql import functions as F

logger = logging.getLogger('Data Ingestion')


def pipe_cleaning_level0(spark_df, spark_df_name, columns_filter_dict):
    """
    Pipe for the data ingestion cleaning operations :
    - Removing duplicates
    - Removing some unwanted columns
    operations.

    :param spark_df: the input SparkDataframe(pyspark.sql.dataframe).
    :param spark_df_name: the name of the SparkDataframe(str).
    :param columns_filter_dict: the dictionary of columns to drop from
    a potential table(dict).
    :return: the output SparkDataframe(pyspark.sql.dataframe).
    """
    # Function's signature.
    func_signature = "\n------> Beginning of the function: pipe_cleaning_level0"
    logger.info("{:<32}".format(func_signature) + '\n')

    # Main operations.

    # 1) Removing duplicates
    if spark_df_name not in ['f_forecast_model']:
        logger.info('\t| a) Dropping duplicated rows.')
        spark_df = spark_df.dropDuplicates()


    # 2) Removing some unwanted columns
    if spark_df_name in columns_filter_dict.keys():

        columns_to_drop = columns_filter_dict[spark_df_name]
        columns_nb_before = len(spark_df.columns)
        columns_to_drop_nb = len(columns_to_drop)

        logger.info("\t| b)Dropping from the table {} columns out of {} "
                    .format(columns_to_drop_nb, columns_nb_before))

        spark_df = spark_df.drop(*columns_to_drop)
        logger.info("\t\t| There are now {} columns left"
                    .format(len(spark_df.columns), columns_nb_before))

    else:
        logger.info("\t| b)There is no need dropping some columns from the table")

    # Function's signature.
    func_signature = "\n------> End of the function: pipe_cleaning_level0"
    logger.info("{:<32}".format(func_signature) + '\n')

    return spark_df


def data_raw_to_clean_s3(bucket_raw, bucket_clean, table_source, bucket_sub_directory, table_directory, table_header, time_filter,
                         spark_session, columns_filter_dict, s3client, pgconf):
    """
    Reads raw data from a s3 bucket, then cleans it and finally
    writes it into another s3 bucket as parquet files.

    :param table_source:
    :param bucket_raw:  the name of the input s3 bucket where raw data is read(str).
    :param bucket_clean: the name of the output s3 bucket where clean data will be written(str).
    :param bucket_sub_directory: the name of the input s3 bucket schema or subdirectory(str).
    :param table_directory: the name of the table to be cleaned(str).
    :param table_header: the table's header (list of tuples <column name, column type, is redshift's primary key>).
    :param time_filter: the number of last months to process(int ). It could take 'Full' value as well(str).
    :param spark_session: the Spark session (spark session).
    :param columns_filter_dict: the dictionary of columns to drop from
    a potential table(dict).
    :param s3client: the boto3 s3 client(boto3 client).
    :param pgconf: the technical conf to retrieve input/output conf
    :return:
    """
    # Function's signature.
    func_signature = "\n------> Beginning of the function: data_raw_to_clean_s3"
    logger.info("{:<32}".format(func_signature) + '\n')
    table_directory_bis = table_source + '/' + bucket_sub_directory + '/' + table_directory + '/'

    # Main operations.

    # Listing all s3 objects inside an s3 path.
    keys_generator = ut.get_all_s3_objects(s3client=s3client, Bucket=bucket_raw, Prefix=table_directory_bis)
    # Converting the generator(a collection) into list.
    keys_generator_list = list(keys_generator)
    # We use itertools.chain.from_iterable to flatten embedded list of s3 keys.
    merged_keys_list = list(itertools.chain.from_iterable(keys_generator_list))
    # Retrieve only s3 objects with specific suffix.
    keys_list = ut.list_files_suffix([s3_key['Key'] for s3_key in merged_keys_list], delimiter='.gz')
    # Test if the table to process does contain a subdirectory.
    has_sub_directory_test = ut.has_sub_directory(keys_list)

    if has_sub_directory_test:
        logger.info("* This table contains sub-directories")

        # Retrieving subdirectories of the table we want to process.
        sub_directory_list = [m for m in
                              set([month.replace(table_directory_bis, '').split('/')[0] for month in
                                   keys_list])]

        # Check  if we are not in full scope.
        if time_filter != "Full":
            logger.info('\t| ====> [Delta scope] Processing the table only for the {}  last month(s)'.format(
                time_filter))
            # Sorting the previous list.
            if time_filter < 0:
                sub_directory_list = sorted(sub_directory_list, reverse=False)
                time_filter = time_filter * -1
            else:
                sub_directory_list = sorted(sub_directory_list, reverse=True)

            sub_directory_list = sub_directory_list[:time_filter]
        else:
            logger.info('\t| ====> [Full scope] Processing the table for all months')
            sub_directory_list = sorted(sub_directory_list, reverse=True)

        # Entering the loop for all subdirectories.
        for i, sub_directory in enumerate(sub_directory_list):
            logger.info('\n' + 90 * '-')
            custom_title = '\n' + str(i + 1) + ") Reading the Table partition: " + sub_directory
            logger.info("{:<32}".format(custom_title) + '\n')

            new_table_path = table_directory_bis + sub_directory
            logger.info("{:<32}".format(new_table_path) + '\n')
            spark_df = ut.read_csv_with_columns(bucket_raw, new_table_path, table_header, spark_session)

            # Calling cleaning pipe function level zero.
            output_df = pipe_cleaning_level0(spark_df, spark_df_name=table_directory,
                                             columns_filter_dict=columns_filter_dict)

            # Exporting the final table as parquet files in the subdirectory.
            output_conf = pgconf.get_datasource_conf(table_directory)['output']
            towrite_df = output_df.repartition(output_conf['nb_partitions']) if 'nb_partitions' in output_conf.keys() \
                else output_df
            export_path = table_source + '/' + table_directory + '/' + output_conf['partition'] + '=' + sub_directory + '/'
            ut.export_parquet_s3(towrite_df, bucket_clean, export_path)

        spark_session.sql('MSCK REPAIR TABLE ' + output_conf["database"] + '.' + output_conf["table"])

    else:
        logger.info("* This table does not contain sub-directories")

        spark_df = ut.read_csv_with_columns(bucket_raw, table_directory_bis, table_header, spark_session)

        # Calling cleaning pipe function level zero
        output_df = pipe_cleaning_level0(spark_df, spark_df_name=table_directory,
                                         columns_filter_dict=columns_filter_dict)

        # Exporting the final table as parquet files.
        output_conf = pgconf.get_datasource_conf(table_directory)['output']
        towrite_df = output_df.repartition(output_conf['nb_partitions']) if 'nb_partitions' in output_conf.keys()\
            else output_df
        export_path = table_source + '/' + table_directory
        ut.export_parquet_s3(towrite_df, bucket_clean, export_path)

    func_signature = "\n------> End of the function: data_raw_to_clean_s3"
    logger.info("{:<32}".format(func_signature) + '\n')


def main_pipe(bucket_raw_data, table_source, bucket_schema, list_table_schema, bucket_cleaned_data, time_filter,
              headers_paths_list, headers_last_sub_folder, spark_session,
              columns_filter_dict, s3client, pgconf):
    """
    main function for the data ingestion process.
    For all tables in the bucket_raw_data's schema, it reads them, then cleans them and finally
    writes them as parquet files into bucket_cleaned_data's schema.

    :param table_source:
    :param bucket_raw_data: the name of the input s3 bucket where raw data is read(str).
    :param bucket_schema: the name of the input s3 bucket schema or subdirectory(str).
    :param list_table_schema: the name of the tables to process(list of str).
    :param bucket_cleaned_data: the name of the output s3 bucket where clean data will be written(str).
    :param time_filter: the number of last months to process(int).It could take 'Full' value as well(str).
    :param headers_paths_list: list of all paths inside the tables' headers directory(list of str).
    :param headers_last_sub_folder: the most recent timestamped sub-folder inside the tables'
    headers directory (integer).
    :param spark_session: the Spark session (spark session).
    :param columns_filter_dict: the dictionary of columns to drop from
    a potential table(dict).
    :param s3client: the boto3 s3 client(boto3 client).
    :return:
    """
    # I) Printing the global pipe title.
    pipe_title = " [Beginning of Data Cleaning level zero for schema *** {} ***] ".format(bucket_schema.upper())
    logger.info('\n' + '{:=^90}'.format(pipe_title))

    # II) Fetching headers for all tables in the schema.
    sub_title = "\n==========> Step 1. Fetching headers for all tables in *** {} ***".format(bucket_schema.upper())
    logger.info("{:<32}".format(sub_title) + '\n')

    dict_table = {table: ut.find_col_name(bucket_name=bucket_raw_data, bucket_subdirectory=bucket_schema,
                                          table_directory=table, headers_paths_list=headers_paths_list,
                                          headers_last_sub_folder=headers_last_sub_folder,
                                          spark_session=spark_session)
                  for table in list_table_schema}

    # III) Launching the main processing function for all tables in the schema
    sub_title = "\n==========> Step 2. Launching global pipeline [data_raw_to_clean_s3] for all tables in *** {} ***". \
        format(bucket_schema.upper())
    logger.info("{:<32}".format(sub_title) + '\n')

    # Loop over all tables in the specific schema.
    for table_directory, table_header in dict_table.items():
        logger.info('\n' + '{:-^90}'.format(" Table [" + table_directory + "]"))
        data_raw_to_clean_s3(bucket_raw=bucket_raw_data, bucket_clean=bucket_cleaned_data, table_source=table_source,
                             bucket_sub_directory=bucket_schema, table_directory=table_directory,
                             table_header=table_header, time_filter=time_filter, spark_session=spark_session,
                             columns_filter_dict=columns_filter_dict, s3client=s3client, pgconf=pgconf)
        logger.info('\n' + 90 * '-')

    logger.info('\n' + 90 * '=')

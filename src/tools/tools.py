import os
import re
import itertools
import logging
import src.tools.constants as cst
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, DoubleType, DecimalType, \
    TimestampType, DateType, \
    BooleanType, \
    StringType

logger = logging.getLogger("Data Ingestion")


def export_parquet_s3(spark_df, bucket_name, table_path):
    """
    Writing SparkDataframe into s3 as parquet files.

    :param spark_df: the input SparkDataframe(pyspark.sql.dataframe).
    :param bucket_name: the name of the output s3 bucket where clean data will be written(str).
    :param table_path: the name of the table to write as parquet files(str).
    :return:
    """
    # Function's signature for printing purpose.
    func_signature = "\n------> Beginning of the function: export_parquet"
    logger.info("{:<32}".format(func_signature) + '\n')

    s3_writing_path = cst.URL_S3 + '{}/{}'.format(bucket_name, table_path)
    logger.info("\t| Writing parquet to path: {}".format(s3_writing_path))

    spark_df.write.parquet(s3_writing_path, mode="overwrite")

    # Function's signature for printing purpose.
    func_signature = "\n------> End of the function: export_parquet"
    logger.info("{:<32}".format(func_signature) + '\n')


def data_type_mapping():
    """
    The mapping dictionary for Spark data types.

    :return: the mapping dictionary(dict).
    """
    # Function's signature for printing purpose.
    func_signature = "\n>> Using the function: data_type_mapping"
    logger.info("{:<32}".format(func_signature) + '\n')

    data_types_mapping = {
        "boolean": BooleanType(),
        "tinyint": ByteType(),
        "bigint": LongType(),
        "integer": IntegerType(),
        "smallint": ShortType(),
        "double": DoubleType(),
        "character": StringType(),
        "date": DateType(),
        "timestamp": TimestampType()}

    return data_types_mapping


def has_sub_directory(keys_list):
    """
    For a given list of paths, checks if the first path contains
    a subdirectory.

    :info: !!! Make sure to adapt it to your project ...

    :param keys_list:  the list of paths(list of str).
    :return: the test result(bool).
    """
    func_signature = "\n>> Using the function: has_sub_directory"
    logger.info("{:<32}".format(func_signature) + '\n')

    length_var = len(keys_list[0].split('/'))

    if length_var < 5:
        return False
    else:
        return True


def list_files_suffix(list_, delimiter):
    """
    listing all paths ending with a specific
    character.

    :param list_: the list of paths.
    :param delimiter: the specific character at the end of a
    path.
    :return: the list of paths considered as valid.
    """
    func_signature = "\n>> Using the function: list_files_suffix"
    logger.info("{:<32}".format(func_signature) + '\n')

    list_files = list()
    for file in list_:
        if file.endswith(delimiter):
            list_files.append(file)
    return list_files


def get_all_s3_objects(s3client, **base_kwargs):
    """
    Listing all s3 Keys.

    :Info:
    Actually, 'list_objects_v2' returns some or all (up to 1,000) of
    the objects in a bucket.You should always check the IsTruncated
    element in the response. If there are no more objects to list, IsTruncated
    is set to false. If there are more objects to list, IsTruncated is set to true,
    and there will be a value in NextContinuationToken.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

    :param s3client: the boto3 s3 client(boto3 client).
    :param base_kwargs: some 'key-value' arguments.
    :return: a collection of s3 Keys(collection).
    """
    continuation_token = None
    # Function's signature for printing purpose.
    func_signature = "\n>> Using the function: get_all_s3_objects"
    logger.info("{:<32}".format(func_signature) + '\n')

    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3client.list_objects_v2(**list_kwargs)
        yield response.get('Contents', [])
        if not response.get('IsTruncated'):
            break
        continuation_token = response.get('NextContinuationToken')


def read_csv_with_columns(bucket_name, table_directory, table_header, spark_session):
    """
    Reads csv file by adding to it headers and data types for columns.

    :param bucket_name: the name of the input s3 bucket where raw data is read(str).
    :param table_directory: the name of the input s3 bucket's schema or subdirectory(str).
    :param table_header: the table's header (list of tuples <column name, column type, is redshift's primary key>).
    :param spark_session: the Spark session (spark session).
    :return: a SparkDataframe(pyspark.sql.dataframe).
    """
    # Function's signature for printing purpose.
    func_signature = "------> Beginning of the function: read_csv_with_columns"
    logger.info("{:<32}".format(func_signature) + '\n')

    s3_reading_path = cst.URL_S3 + '{}/{}'.format(bucket_name, table_directory)
    # Sanity-check
    logger.info("\t***!!! Coherence test : Does the number of columns in the header file match that in the real table?")
    dummy_df = spark_session.read.csv(s3_reading_path, sep=";")
    nb_columns = str(dummy_df.take(1)[0]).count('|') + 1
    assert len(table_header) == nb_columns, "The number of columns in the header file doesn't match that in the real " \
                                            "table "
    logger.info("\t***!!! Coherence test [OK]: The table contains {} columns!".format(nb_columns))

    struct_field_list = []
    data_type_mapping_dict = data_type_mapping()
    for elt in table_header:
        col_name = elt[0]
        col_type_key = elt[1]
        is_nullable = elt[2] == "false"  # if elt[2] is "false", this means  the column is not a redshift primary key
        # so it can be nullable, so in this case "is_nullable=True".

        # For 'numeric' columns, retrieve the decimal precisions in order to create the right Spark data type
        # equivalent.
        if col_type_key.startswith('numeric'):
            decimal_precision = [int(s) for s in re.findall(r'-?\d+\.?\d*', col_type_key)]
            col_type = DecimalType(precision=decimal_precision[0], scale=decimal_precision[1])
        else:
            col_type = data_type_mapping_dict[col_type_key]
        # For all columns, add their data structures to the list 'struct_field_list'.
        struct_field_list.append(StructField(name=col_name, dataType=col_type, nullable=is_nullable))
    # Defining the final tables's schema.
    table_schema = StructType(struct_field_list)

    logger.info("\t| Reading from path: {}".format(s3_reading_path))
    # Reading the csv file with the right columns and data types.
    spark_df = spark_session.read.csv(s3_reading_path, schema=table_schema, sep='|')

    # Function's signature for printing purpose.
    func_signature = "\n------> End of the function: read_csv_with_columns"
    logger.info("{:<32}".format(func_signature) + '\n')
    return spark_df


def reformat_dtypes_names(x):
    """
    For a given string, retrieves the first word.
    Returns:

    :param x: the input string(str).
    :return: the first word within the string(str).
    """
    res = x.lower().split(' ')[0]
    if res.startswith('character('):
        res = 'character'
    return res


def get_timestamped_headers_folder(bucket_name, s3client):
    """
    Extracting the list of all paths inside the tables' headers directory (Ex: s3a://fcs-raw/TABLE_COLUMN) as well as
    the most recent timestamped sub-folder of this tables' headers directory(Ex: s3a://fcs-raw/TABLE_COLUMN/202009).

    :param bucket_name: the name of the input s3 bucket where tables' headers directory is located(str).
    :param s3client: the boto3 s3 client(boto3 client).
    :return: the most recent timestamped sub-folder of tables' headers (integer) and the
    list of all paths inside the tables' header directory(list of str).
    """
    # Function's signature for printing purpose.
    func_signature = "\n------> Beginning of the function: get_timestamped_headers_folder"
    logger.info("{:<32}".format(func_signature) + '\n')

    # Retrieving the name of the tables' headers directory.
    path_s3 = cst.PATH_S3_COLUMN

    # Get a collection of all s3 items inside the path_s3 directory.
    keys_generator = get_all_s3_objects(s3client=s3client, Bucket=bucket_name, Prefix=path_s3)
    # Converting the generator(a collection) to a list.
    keys_generator_list = list(keys_generator)
    # We use itertools.chain.from_iterable to flatten embedded list of s3 keys.
    merged_keys_list = list(itertools.chain.from_iterable(keys_generator_list))
    # Removing duplicates inside 'merged_keys_list'.
    list_files = set([object_key['Key'] for object_key in merged_keys_list])
    # Get the more recent sub-directory.
    list_month = [''.join([i for i in f if i.isdigit()][:6]) for f in list_files]
    last_directory = max([int(m) for m in list_month if m != ''])

    func_signature = "\n------> End of the function: get_timestamped_headers_folder"
    logger.info("{:<32}".format(func_signature) + '\n')

    return list_files, last_directory


def find_col_name(bucket_name, bucket_subdirectory, table_directory, headers_paths_list,
                  headers_last_sub_folder, spark_session):
    """
    Getting table header (columns' names, data types and the fact that fields could be null or not).

    :param bucket_name: the name of the input s3 bucket where raw data is readen(str).
    :param bucket_subdirectory:
    the name of the input s3 bucket schema or subdirectory(str).
    :param table_directory: the name of the table(str).
    :param headers_paths_list: the list of all paths inside the tables' headers directory(list of str).
    :param headers_last_sub_folder: the most recent timestamped sub-folder inside the tables' headers directory
    (integer).
    :param spark_session: the Spark session (spark session).
    :return: the table's header (list of tuples <column
    name, column type, is redshift's primary key>).
    """
    # Function's signature for printing purpose.
    func_signature = "\n------> Beginning of the function: find_col_name"
    logger.info("{:<32}".format(func_signature) + '\n')

    # Retrieving the name of the tables' headers directory.
    path_s3 = cst.PATH_S3_COLUMN

    # Check if the table's path we're looking for is inside the sub-directory path_s3.
    path_list_last_files = [f for f in headers_paths_list if
                            path_s3 + str(headers_last_sub_folder) + '/' + bucket_subdirectory + '/' + table_directory == f]

    # Otherwise, look for a table's path ending with '_current' tag.
    if len(path_list_last_files) == 0:
        path_list_last_files = [f for f in headers_paths_list if path_s3 + str(
            headers_last_sub_folder) + '/' + bucket_subdirectory + '/' + table_directory + '_current' == f]
    # Choosing the most recent subdirectory where to read the table's header csv.
    path_last_files = path_list_last_files[0]

    # Schema for reading the header file.
    my_schema = StructType([
        StructField("col_name", StringType(), False),
        StructField("col_type", StringType(), False),
        StructField("is_redshift_pk", StringType(), False)])
    # the variable 'is_redshift_pk' stands for 'redShift primary key' which is not 'nullable'. If
    # 'is_redshift_pk' == 'false' this means the column is nullable.

    # Definition of the udf function (a customized Spark's function).
    reformat_dtypes_names_func = F.udf(lambda x: reformat_dtypes_names(x), StringType())

    logger.info("\t| Fetching and returning header for table : {}".format(table_directory))

    # We read the header file which gives us 3 string columns : 'col_name', 'col_type' and 'is_redshift_pk'.
    df_cols_struct_field = spark_session.read.load(cst.URL_S3 + bucket_name + '/' + path_last_files,
                                                   schema=my_schema, format="csv", sep=";")
    # We then reformat the 'col_type' aiming at retrieving just the first word of the string 'col_type'.
    df_cols_struct_field = df_cols_struct_field.withColumn('type_reformat', reformat_dtypes_names_func("col_type"))
    # For each column name (rows of the header file) we create a tuple
    # <column name, column type, is redshift's primary key>
    # saved in the column 'struct_field'.
    df_cols_struct_field = df_cols_struct_field.select(
        F.array("col_name", "type_reformat", "is_redshift_pk").alias('struct_field'))
    # The list of tuples <column name, column type, is redshift's primary key> extracted from  'struct_field' column.
    cols_struct_field = df_cols_struct_field.rdd.flatMap(lambda x: x).collect()

    # Function's signature for printing purpose.
    func_signature = "\n------> End of the function: find_col_name"
    logger.info("{:<32}".format(func_signature) + '\n')

    return cols_struct_field

CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_general_data_customer (
    cust_id                        STRING,
    sap_source                     STRING,
    flag_one_time_account          STRING,
    address_num                    STRING,
    cust_block                     STRING,
    ean_1                          STRING,
    ean_2                          STRING,
    ean_3                          STRING,
    communication_num_deliver_cust STRING,
    date_creation                  DATE,
    creation_editor                STRING,
    cust_account_grp               STRING,
    location_id                    STRING,
    flag_central_deletion          STRING,
    plant_id                       STRING,
    rs_technical_date              TIMESTAMP,
    rs_technical_flow              STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_general_data_customer/'
;
CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_listing_assortment (
    plant_id          STRING,
    material_id       STRING,
    sales_unit        STRING,
    date_valid_to     DATE,
    sequence_num      DECIMAL(3,0),
    sap_source        STRING,
    date_valid_from   DATE,
    date_last_change  DATE,
    assortment_status STRING ,
    model_id          STRING,
    rs_technical_date TIMESTAMP,
    rs_technical_flow STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_listing_assortment/'
;
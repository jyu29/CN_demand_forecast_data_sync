CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.physical_stock_snapshot (
    material_id         STRING,
    plant_id            STRING,
    sap_source          STRING,
    date_current        DATE,
    local_currency      STRING,
    bum                 STRING,
    purch_org_stock_ttc decimal(19, 2),
    stock_qtt_bum       decimal(15, 3)
)
PARTITIONED BY (month STRING)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/physical_stock_snapshot/'
;
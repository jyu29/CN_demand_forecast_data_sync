CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.apo_sku_mrp_status_h (
   mandt             STRING,
   sku               DECIMAL(10,0),
   custom_zone       STRING,
   status            STRING,
   date_begin        DATE,
   date_end          DATE,
   rs_technical_date TIMESTAMP,
   rs_technical_flow STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/apo_sku_mrp_status_h/'
;
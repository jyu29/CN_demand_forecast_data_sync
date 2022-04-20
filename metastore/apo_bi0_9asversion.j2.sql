CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.apo_bi0_9asversion (
    sid           INT,
    bi0_9aversion STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/apo_bi0_9asversion/'
;
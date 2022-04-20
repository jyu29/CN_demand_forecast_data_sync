CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.apo_bic_szd_model (
    sid          INT,
    bic_zd_model STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/apo_bic_szd_model/'
;
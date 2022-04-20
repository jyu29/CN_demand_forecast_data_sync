CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_selection(
    selection_id      DECIMAL(10,0),
    comm_season_id    DECIMAL(6,0),
    type_selection    STRING,
    rs_technical_date TIMESTAMP,
    rs_technical_flow STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_selection/'
;
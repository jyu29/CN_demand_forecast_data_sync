CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_label_translation (
    lat_idr_label_translation BIGINT,
    lab_idr_label             BIGINT,
    tlb_typ_label             STRING,
    lab_num_label             DECIMAL(10,0),
    lan_code_langue_lan       STRING,
    lat_short_label           STRING,
    lat_long_label            STRING,
    lat_date_creation         TIMESTAMP,
    lat_date_upd              TIMESTAMP,
    lat_date_upd_tec          TIMESTAMP,
    rs_technical_date         TIMESTAMP,
    rs_technical_flow         STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_label_translation/'
;
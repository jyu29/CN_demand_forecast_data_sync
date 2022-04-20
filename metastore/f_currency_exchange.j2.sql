CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.f_currency_exchange (
    cpt_idr_cur_price       SMALLINT,
    cur_idr_currency_base   SMALLINT,
    cur_idr_currency_restit SMALLINT,
    hde_effect_date         TIMESTAMP,
    hde_end_date            TIMESTAMP,
    hde_share_price         DECIMAL(15,7),
    system_date             TIMESTAMP,
    rs_technical_date       TIMESTAMP,
    rs_technical_flow       STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/f_currency_exchange/'
;
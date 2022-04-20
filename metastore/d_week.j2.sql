CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_week (
    wee_id_week          STRING,
    wee_id_week_comp     STRING,
    wee_num_week         STRING,
    day_first_day_week   DATE,
    day_last_day_week    DATE,
    mon_id_month         STRING,
    mon_id_month_comp    STRING,
    mon_num_month        STRING,
    str_id_semestre      STRING,
    str_id_semestre_comp STRING,
    str_num_semestre     STRING,
    yea_id_year          STRING,
    rs_technical_date    TIMESTAMP,
    rs_technical_flow    STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_week/'
;
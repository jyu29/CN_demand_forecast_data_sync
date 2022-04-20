CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.f_range_choice (
    yea_id_year           decimal(4,0),
    wee_id_week           STRING,
    start_week            STRING,
    end_week              STRING,
    season_id             decimal(10,0),
    fam_idr_family        INT,
    rcf_num_family        decimal(10,0),
    range_level           STRING,
    rcf_density           STRING,
    but_idr_business_unit decimal(19,0),
    rcf_num_business_unit decimal(10,0),
    tir_nom_zd            STRING,
    rcf_flg_validated     decimal(1,0),
    linear_meter          decimal(6,2),
    square_meter          decimal(6,2),
    nb_model              decimal(10,0),
    rs_technical_date     Timestamp,
    rs_technical_flow     STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/f_range_choice/'
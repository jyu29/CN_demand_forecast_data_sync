CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_comm_season (
    comm_season_id         DECIMAL(10,0),
    custom_zone_type_tiers DECIMAL(5,0),
    custom_zone_tiers      DECIMAL(9,0),
    custom_zone_sous_tiers DECIMAL(9,0),
    date_start_season      DECIMAL(9,0),
    date_end_season        DECIMAL(9,0),
    label_season           STRING,
    dmi_family_lvl         DECIMAL(10,0),
    dmi_family_node        DECIMAL(10,0),
    dmi_family_org         DECIMAL(10,0),
    year_season            DECIMAL(4,0),
    rs_id                  STRING,
    rs_technical_date      TIMESTAMP,
    rs_technical_flow      STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_comm_season/'
;
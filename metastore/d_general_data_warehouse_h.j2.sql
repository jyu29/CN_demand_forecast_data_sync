CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_general_data_warehouse_h (
    date_begin                DATE,
    date_end                  DATE,
    sdw_material_id           STRING,
    sdw_plant_id              STRING,
    sdw_sap_source            STRING,
    sdw_code_abc              STRING,
    sdw_purch_grp             STRING,
    sdw_material_mrp          STRING,
    sdw_time_delivery         STRING,
    sdw_period_indicator      STRING,
    sdw_safety_stock          STRING,
    sdw_import_code           STRING,
    sdw_country_origin        STRING,
    sdw_profit_center         STRING,
    sdw_planification_horizon STRING,
    sdw_covert_profil         STRING,
    sdw_safety_time           STRING,
    sdw_unit_grp              STRING,
    sdw_handling_group        STRING,
    sdw_min_safety_stock      STRING,
    rs_technical_date         TIMESTAMP,
    rs_technical_flow         STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_general_data_warehouse_h/'
;

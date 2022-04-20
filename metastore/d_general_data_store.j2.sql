CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_general_data_store (
    sds_material_id                       STRING,
    sds_plant_id                          STRING,
    sds_sap_source                        STRING,
    sds_maintenance_status                STRING,
    sds_material_status                   STRING,
    sds_code_abc                          STRING,
    sds_purch_grp                         STRING,
    sds_mrp_type                          STRING,
    sds_time_delivery                     INT,
    sds_period_indicator                  STRING,
    sds_safety_stock                      INT,
    sds_rounding_value                    INT,
    sds_max_storage                       INT,
    sds_source_supply                     STRING,
    sds_import_code                       STRING,
    sds_country_origin                    STRING,
    sds_profit_center                     STRING,
    sds_covert_profil                     STRING,
    sds_rounding_profil                   STRING,
    sds_safety_time                       INT,
    sds_unit_grp                          STRING,
    sds_min_safety_stock                  INT,
    rs_technical_date                     DATE,
    rs_technical_flow                     STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_general_data_store/'
;
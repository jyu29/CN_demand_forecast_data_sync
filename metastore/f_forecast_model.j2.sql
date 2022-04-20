CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.f_forecast_model (
    calculation_date         DATE,
    date_week_year           INT,
    model_sid                STRING,
    custom_zone_sid          STRING,
    planning_version_apo_sid STRING,
    corrected_sales_histo    INT,
    global_demand            INT,
    sales_histo              INT,
    stat_forecast            INT
)
PARTITIONED BY (partition_date STRING)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/f_forecast_model/'
;
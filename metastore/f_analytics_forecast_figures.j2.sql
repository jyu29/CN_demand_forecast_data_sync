CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.f_analytics_forecast_figures (
    calculation_date               DATE,
    week                           DATE,
    plant_id                       STRING,
    material_id                    STRING,
    stock_average                  DECIMAL(19,6),
    weekly_mean_fcst_1             DECIMAL(19,6),
    weekly_mean_fcst_2             DECIMAL(19,6),
    weekly_mean_fcst_3             DECIMAL(19,6),
    extra_fsct_period_1            DECIMAL(3,0),
    extra_fsct_period_2            DECIMAL(3,0),
    consumption_base_unit          DECIMAL(19,6),
    stock_unit_quantity            STRING,
    sales_value                    DECIMAL(15,5),
    currency                       STRING,
    actual_coverage_fact_or_days   DECIMAL(11,2),
    actual_coverage_fact_or_period DECIMAL(11,2),
    replenishment_lead_time        DECIMAL(7,2),
    source_location_internal_id    STRING,
    location_type                  STRING,
    replenishment_type             STRING,
    weekly_dif_effect              DECIMAL(19,0),
    listingstatus                  DECIMAL(1,0),
    weekly_fnr_record_is_completed STRING,
    source_location                STRING,
    weekly_active_fcst             DECIMAL(19,6),
    active_forecast_type           DECIMAL(1,0),
    analytics_substitution         STRING,
    rs_technical_date              TIMESTAMP,
    rs_technical_flow              STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/f_analytics_forecast_figures/'
;
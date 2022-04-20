CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_model_data_selection(
    selection_element_id            DECIMAL(15,0),
    project_model_id                DECIMAL(15,0),
    selection_id                    DECIMAL(15,0),
    brand_label                     STRING,
    date_start_sales                DECIMAL(9,0),
    date_end_sales                  DECIMAL(9,0),
    design_id                       STRING,
    ref_model_id                    STRING,
    typology                        STRING,
    range_lvl                       DECIMAL(9,0),
    custom_zone_estimated_purch_qtt DECIMAL(10,0),
    status_price                    STRING,
    flag_stop_end_season            STRING,
    date_start_link                 DECIMAL(9,0),
    date_end_link                   DECIMAL(9,0),
    start_num_store                 DECIMAL(9,0),
    end_num_store                   DECIMAL(9,0),
    flag_selection_active           STRING,
    milestone_id                    DECIMAL(4,0),
    brand_id                        DECIMAL(10,0),
    rs_technical_date               TIMESTAMP,
    rs_technical_flow               STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_model_data_selection/'
;
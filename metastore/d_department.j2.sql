CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_department (
    dpt_idr_department         SMALLINT,
    unv_idr_univers            SMALLINT,
    org_num_organisation_level SMALLINT,
    lab_idr_label              INT,
    dpt_num_department         SMALLINT,
    dpt_flag_commercial        SMALLINT,
    dpt_date_creation          TIMESTAMP,
    dpt_date_upd               TIMESTAMP,
    dpt_user                   STRING,
    dpt_num_order              DECIMAL(10,0),
    dpt_date_begin             DATE,
    dpt_date_end               DATE,
    rs_technical_date          TIMESTAMP,
    rs_technical_flow          STRING,
    unv_num_univers            DECIMAL(10,0),
    lab_idr_label_unv          DECIMAL(10,0)
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_department/'
;
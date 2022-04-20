CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_country(
    cnt_idr_country                SMALLINT,
    cnt_country_code               STRING,
    cnt_country_code_3a            STRING,
    cnt_num_country                DECIMAL(10,0),
    tlb_typ_label                  STRING,
    lab_idr_label_cnt              INT,
    cur_idr_currency               SMALLINT,
    lan_idr_language               INT,
    lan_language_code              STRING,
    lan_idr_language2              INT,
    lan_language_code2             STRING,
    lan_idr_language3              INT,
    lan_language_code3             STRING,
    cnt_update_date                date,
    cnt_cee                        STRING,
    cnt_country_risk_degree        STRING,
    bank_code                      STRING,
    cnt_establish_country_code     DECIMAL(10,0),
    cnt_producer_country           STRING,
    cnt_street_location            STRING,
    cnt_postcode_location          STRING,
    cnt_producer_country_deca_code DECIMAL(10,0),
    rs_technical_date              TIMESTAMP,
    rs_technical_flow              STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_country/'
;
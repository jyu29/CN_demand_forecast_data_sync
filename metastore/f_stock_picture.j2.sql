CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.f_stock_picture (
    sku_idr_sku                BIGINT,
    but_idr_business_unit      BIGINT,
    stt_idr_stock_type         BIGINT,
    spr_date_stock             TIMESTAMP,
    spr_date_shipping          TIMESTAMP,
    f_quantity                 DECIMAL(10,0),
    f_amt_cost_pri_tax_ex      DECIMAL(18,6),
    f_amt_cost_pri_tax_ex_fisc DECIMAL(18,6),
    f_amt_pri_tax_in           DECIMAL(18,6),
    f_amt_pri_tax_ex           DECIMAL(18,6),
    f_wap_fisc_last            DECIMAL(18,6),
    f_wap_last                 DECIMAL(18,6),
    cur_idr_currency_sale      BIGINT,
    cur_idr_currency_cost      BIGINT,
    cur_idr_currency_wap       BIGINT,
    stk_flag_calc              SMALLINT,
    rs_technical_date          TIMESTAMP,
    rs_technical_flow          STRING
)
PARTITIONED BY (month STRING)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/f_stock_picture/'
;

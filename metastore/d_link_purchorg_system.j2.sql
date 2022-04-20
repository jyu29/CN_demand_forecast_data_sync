CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_link_purchorg_system (
    stp_purch_org_legacy_id  STRING,
    stp_purch_org_highway_id STRING,
    stp_purch_org_md         STRING,
    stp_purch_org_other      STRING,
    stp_sales_area_fms       STRING,
    rs_technical_date        TIMESTAMP,
    rs_technical_flow        STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_link_purchorg_system/'
;
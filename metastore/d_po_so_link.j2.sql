CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_po_so_link (
    purch_org_id      STRING,
    sales_org_id      STRING,
    sap_source        STRING,
    rs_technical_date DATE,
    rs_technical_flow STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_po_so_link/'
;
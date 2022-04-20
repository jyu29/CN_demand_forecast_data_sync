CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.ecc_zaa_extplan (
   mandt             STRING,
   ekorg             STRING,
   satnr             STRING,
   matnr             STRING,
   matkl             STRING,
   zzmrp             STRING,
   zzaposnp          STRING,
   mrp_pr            STRING,
   zzcacp            STRING,
   dispr             STRING,
   dismm             STRING,
   erdat             DATE,
   zmdat             DATE,
   zcession          STRING,
   lifestatus        STRING,
   rs_technical_date TIMESTAMP,
   rs_technical_flow STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/ecc_zaa_extplan/'
;
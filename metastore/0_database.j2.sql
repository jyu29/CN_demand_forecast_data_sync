CREATE SCHEMA IF NOT EXISTS fcst_clean_${env}
LOCATION 's3://fcst-clean-${env}/datalake/'
;
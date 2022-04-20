CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.sales_data_material (
    material_id                                STRING,
    sales_org                                  STRING,
    distrib_channel                            STRING,
    sap_source                                 STRING,
    flag_material_deletion_distrib_chain_level STRING,
    item_cat_grp_material_master               STRING,
    account_assigment_grp_material             STRING,
    assortment_grade                           STRING,
    display_min_small                          DECIMAL(7,0),
    display_min_average                        DECIMAL(7,0),
    display_min_large                          DECIMAL(7,0),
    small_linear_capacity                      DECIMAL(7,0),
    large_linear_capacity                      DECIMAL(7,0),
    extra_linear_capacity                      DECIMAL(7,0),
    breaking_su_allowed                        STRING,
    grouping_sunit_pcb                         STRING,
    tracabilty                                 STRING,
    date_start_link_model                      DATE,
    date_end_link_model                        DATE,
    lifestage                                  STRING,
    date_modification_stage                    DATE,
    old_material_id                            STRING,
    implantation_qtt_large_dept                DECIMAL(7,0),
    implantation_qtt_average_dept              DECIMAL(7,0),
    implantation_qtt_small_dept                DECIMAL(7,0),
    date_qi_validation                         DATE
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/sales_data_material/'
;
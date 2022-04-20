CREATE EXTERNAL TABLE IF NOT EXISTS fcst_clean_${env}.d_sku (
    sku_idr_sku               BIGINT,
    sku_num_sku               BIGINT,
    mdl_idr_model             BIGINT,
    mdl_num_model             BIGINT,
    mdl_num_model_r3          BIGINT,
    mdl_blue_product          INT,
    lab_idr_label_mdl         BIGINT,
    mdl_label                 STRING,
    fam_idr_family            BIGINT,
    fam_num_family            BIGINT,
    lab_idr_label_fam         BIGINT,
    family_label              STRING,
    sdp_idr_sub_department    BIGINT,
    sdp_num_sub_department    BIGINT,
    lab_idr_label_sdp         BIGINT,
    sdp_label                 STRING,
    dpt_idr_department        BIGINT,
    dpt_num_department        BIGINT,
    lab_idr_label_dpt         BIGINT,
    dpt_label                 STRING,
    unv_idr_univers           BIGINT,
    unv_num_univers           BIGINT,
    lab_idr_label_unv         BIGINT,
    unv_label                 STRING,
    pnt_idr_product_nature    DECIMAL(7,0),
    pnt_num_product_nature    DECIMAL(7,0),
    lab_idr_label_pnt         BIGINT,
    product_nature_label      STRING,
    nat_idr_element_nature    BIGINT,
    nat_num_element_nature    BIGINT,
    lab_idr_label_nat         BIGINT,
    nature_label              STRING,
    category_label            STRING,
    dsm_code                  STRING,
    grid_size                 STRING,
    ipd_net_weight            DOUBLE,
    ipd_gross_weight          DOUBLE,
    ipd_weight_unit           STRING,
    ipd_width                 DOUBLE,
    ipd_height                DOUBLE,
    ipd_length                DOUBLE,
    ipd_dimension_unit        STRING,
    sku_date_creation         TIMESTAMP,
    sku_date_upd              TIMESTAMP,
    sku_num_order             STRING,
    sku_num_sku_r3            BIGINT,
    brd_num_brand             DECIMAL(10,0),
    brd_label_brand           STRING,
    brd_type_brand_libelle    STRING,
    brd_type_brand            DECIMAL(1,0),
    sku_ean_num               STRING,
    sku_date_begin            TIMESTAMP,
    sku_date_end              TIMESTAMP,
    rs_technical_date         TIMESTAMP,
    rs_technical_flow         STRING
)
STORED AS PARQUET
LOCATION 's3://fcst-clean-${env}/datalake/d_sku/'
;
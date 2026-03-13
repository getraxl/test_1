{{
    config(
        materialized='table'
    )
}}

SELECT
    id::NUMBER(10,0)                        AS id,
    customer_id::NUMBER(10,0)               AS customer_id,
    line1::VARCHAR(50)                      AS line1,
    line2::VARCHAR(50)                      AS line2,
    line3::VARCHAR(50)                      AS line3,
    territory_region_id::NUMBER(10,0)       AS territory_region_id,
    territory_id::NUMBER(10,0)              AS territory_id,
    line6::VARCHAR(50)                      AS line6,
    category_bit_sum::NUMBER(10,0)          AS category_bit_sum,
    valid::BOOLEAN                          AS valid,
    payer_number::VARCHAR(10)               AS payer_number,
    is_new_bapi_mapping_used::BOOLEAN       AS is_new_bapi_mapping_used,
    status_id::NUMBER(10,0)                 AS status_id,
    created_date::TIMESTAMP_NTZ             AS created_date,
    created_by::NUMBER(10,0)               AS created_by,
    updated_date::TIMESTAMP_NTZ             AS updated_date,
    updated_by::NUMBER(10,0)                AS updated_by,
    sap_customer_number::VARCHAR(10)        AS sap_customer_number,
    address_type::VARCHAR(20)               AS address_type,
    is_primary::BOOLEAN                     AS is_primary
FROM {{ source('sqlserver_raw', 'customer_address') }}

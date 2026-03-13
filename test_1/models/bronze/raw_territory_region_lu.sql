{{
    config(
        materialized='table'
    )
}}

SELECT
    id::NUMBER(10,0)                AS id,
    description::VARCHAR(50)        AS description,
    code::VARCHAR(3)                AS code,
    territory_id::NUMBER(10,0)      AS territory_id,
    status_id::NUMBER(10,0)         AS status_id,
    created_date::TIMESTAMP_NTZ     AS created_date,
    created_by::NUMBER(10,0)        AS created_by,
    updated_date::TIMESTAMP_NTZ     AS updated_date,
    updated_by::NUMBER(10,0)        AS updated_by
FROM {{ source('sqlserver_raw', 'territory_region_lu') }}

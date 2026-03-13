{{
    config(
        materialized='table'
    )
}}

SELECT
    id::NUMBER(10,0)                AS id,
    description::VARCHAR(50)        AS description,
    country_code::VARCHAR(3)        AS country_code,
    projection_code::VARCHAR(3)     AS projection_code,
    map_code::VARCHAR(3)            AS map_code,
    comment::VARCHAR(255)           AS comment,
    is_territory::BOOLEAN           AS is_territory,
    postal_code_rule::BOOLEAN       AS postal_code_rule,
    postal_code_mask::VARCHAR(100)  AS postal_code_mask,
    status_id::NUMBER(10,0)         AS status_id,
    created_date::TIMESTAMP_NTZ     AS created_date,
    created_by::NUMBER(10,0)        AS created_by,
    updated_date::TIMESTAMP_NTZ     AS updated_date,
    updated_by::NUMBER(10,0)        AS updated_by
FROM {{ source('sqlserver_raw', 'territory_lu') }}

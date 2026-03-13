{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH address_enriched AS (

    SELECT
        {{ create_key_from_source_id("COALESCE(ca.id, -1)") }} AS address_key,
        ca.id AS source_address_id,
        COALESCE(ca.line1, 'N/A') AS customer_address1,
        COALESCE(ca.line2, 'N/A') AS customer_address2,
        COALESCE(NULLIF(tt.description, 'Data Error'), 'N/A') AS country,
        COALESCE(tr.description, 'N/A') AS region,
        COALESCE(ca.line3, 'N/A') AS city,
        COALESCE(ca.line6, 'N/A') AS post_code,
        ca.address_type AS address_type,
        ca.is_primary AS is_primary
    FROM {{ ref('raw_customer_address') }} ca
    LEFT JOIN {{ ref('raw_territory_lu') }} tt
        ON tt.id = ca.territory_id
    LEFT JOIN {{ ref('raw_territory_region_lu') }} tr
        ON tr.id = ca.territory_region_id

    UNION ALL

    SELECT
        {{ create_key_from_source_id("-1") }} AS address_key,
        NULL AS source_address_id,
        'N/A' AS customer_address1,
        'N/A' AS customer_address2,
        'N/A' AS country,
        'N/A' AS region,
        'N/A' AS city,
        'N/A' AS post_code,
        NULL::VARCHAR(20) AS address_type,
        NULL::BOOLEAN AS is_primary

)

SELECT
    address_key,
    source_address_id,
    customer_address1,
    customer_address2,
    country,
    region,
    city,
    post_code,
    address_type,
    is_primary,
    {{ get_hash_dim_address(
        'address_key',
        'source_address_id',
        'customer_address1',
        'customer_address2',
        'country',
        'region',
        'city',
        'post_code'
    ) }} AS hash_key
FROM address_enriched

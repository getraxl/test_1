{{
    config(
        materialized='incremental',
        unique_key='address_key',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        merge_update_columns=[
            'source_address_id',
            'customer_address1',
            'customer_address2',
            'country',
            'region',
            'city',
            'post_code',
            'hash_key',
            'deleted_on',
            'address_type',
            'is_primary'
        ],
        schema='mart'
    )
}}

{% if is_incremental() %}

WITH silver AS (

    SELECT
        address_key,
        source_address_id,
        customer_address1,
        customer_address2,
        country,
        region,
        city,
        post_code,
        hash_key,
        address_type,
        is_primary
    FROM {{ ref('int_address__enriched') }}

),

incoming AS (

    SELECT
        s.address_key,
        s.source_address_id,
        s.customer_address1,
        s.customer_address2,
        s.country,
        s.region,
        s.city,
        s.post_code,
        s.hash_key,
        s.address_type,
        s.is_primary,
        NULL::TIMESTAMP_NTZ AS deleted_on
    FROM silver s
    LEFT JOIN {{ this }} t
        ON s.address_key = t.address_key
    WHERE t.address_key IS NULL
       OR t.hash_key <> s.hash_key

),

soft_deletes AS (

    SELECT
        t.address_key,
        t.source_address_id,
        t.customer_address1,
        t.customer_address2,
        t.country,
        t.region,
        t.city,
        t.post_code,
        t.hash_key,
        t.address_type,
        t.is_primary,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS deleted_on
    FROM {{ this }} t
    WHERE t.deleted_on IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM silver s WHERE s.address_key = t.address_key
      )

)

SELECT * FROM incoming

UNION ALL

SELECT * FROM soft_deletes

{% else %}

SELECT
    address_key,
    source_address_id,
    customer_address1,
    customer_address2,
    country,
    region,
    city,
    post_code,
    hash_key,
    address_type,
    is_primary,
    NULL::TIMESTAMP_NTZ AS deleted_on
FROM {{ ref('int_address__enriched') }}

{% endif %}

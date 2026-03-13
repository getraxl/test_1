{% macro get_hash_dim_address(address_key, source_address_id, customer_address1, customer_address2, country, region, city, post_code) %}
    SHA2(
        COALESCE(CAST({{ address_key }} AS VARCHAR), '')
        || '|' || COALESCE(CAST({{ source_address_id }} AS VARCHAR), '')
        || '|' || COALESCE(CAST({{ customer_address1 }} AS VARCHAR), '')
        || '|' || COALESCE(CAST({{ customer_address2 }} AS VARCHAR), '')
        || '|' || COALESCE(CAST({{ country }} AS VARCHAR), '')
        || '|' || COALESCE(CAST({{ region }} AS VARCHAR), '')
        || '|' || COALESCE(CAST({{ city }} AS VARCHAR), '')
        || '|' || COALESCE(CAST({{ post_code }} AS VARCHAR), ''),
        512
    )
{% endmacro %}

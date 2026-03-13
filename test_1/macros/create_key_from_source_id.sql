{% macro create_key_from_source_id(source_id) %}
    ABS(HASH(SHA1(CAST({{ source_id }} AS VARCHAR))))
{% endmacro %}

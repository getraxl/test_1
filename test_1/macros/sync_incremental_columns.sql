{% macro sync_incremental_columns(model_database, model_schema, model_table, source_schema, source_table) %}

    {% set source_columns = adapter.get_columns_in_relation(
        api.Relation.create(
            database=model_database, schema=source_schema, identifier=source_table
        )
    ) %}

    {% set target_columns = adapter.get_columns_in_relation(
        api.Relation.create(
            database=model_database, schema=model_schema, identifier=model_table
        )
    ) %}

    {% set target_col_names = target_columns | map(attribute='name') | map('upper') | list %}

    {% for col in source_columns %}
        {% if col.name | upper not in target_col_names %}
            {% set ddl %}
                ALTER TABLE {{ model_database }}.{{ model_schema }}.{{ model_table }}
                ADD COLUMN {{ col.name }} {{ col.data_type }}
            {% endset %}
            {% do run_query(ddl) %}
            {{ log("Added missing column: " ~ col.name ~ " (" ~ col.data_type ~ ") to " ~ model_schema ~ "." ~ model_table, info=True) }}
        {% endif %}
    {% endfor %}

{% endmacro %}

{% macro snowflake__get_drop_sql(relation) %}

    {% if relation.is_dynamic_table %}
        {{ snowflake__get_drop_dynamic_table_sql(relation) }}

    {% elif relation.is_hybrid_table %}
        {{ snowflake__get_drop_hybrid_table_sql(relation) }}

    {% else %}
        {{ default__get_drop_sql(relation) }}

    {% endif %}

{% endmacro %}

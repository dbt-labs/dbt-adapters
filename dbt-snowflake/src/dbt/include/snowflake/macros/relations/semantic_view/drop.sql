{% macro snowflake__get_drop_semantic_view_sql(relation) %}
    drop semantic view if exists {{ relation }}
{% endmacro %}

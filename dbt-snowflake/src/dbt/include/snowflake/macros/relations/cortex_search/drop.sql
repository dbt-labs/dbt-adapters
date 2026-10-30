{% macro snowflake__get_drop_cortex_search_service_sql(relation) %}
    drop cortex search service if exists {{ relation }}
{% endmacro %}

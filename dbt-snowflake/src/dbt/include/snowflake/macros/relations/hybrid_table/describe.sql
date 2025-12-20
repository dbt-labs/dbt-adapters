{% macro snowflake__describe_hybrid_table(relation) %}
    {%- set results = adapter.describe_hybrid_table(relation) -%}
    {%- do return(results) -%}
{% endmacro %}

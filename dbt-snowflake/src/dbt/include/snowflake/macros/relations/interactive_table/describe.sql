{% macro snowflake__describe_interactive_table(relation) %}
    {%- set results = adapter.describe_interactive_table(relation) -%}
    {%- do return(results) -%}
{% endmacro %}

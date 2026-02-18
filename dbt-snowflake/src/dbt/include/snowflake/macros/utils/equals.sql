{% macro snowflake__equals(expr1, expr2) -%}
    ({{ expr1 }} IS NOT DISTINCT FROM {{ expr2 }})
{%- endmacro %}

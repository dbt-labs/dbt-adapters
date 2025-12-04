{% macro snowflake__get_drop_hybrid_table_sql(relation) -%}
    drop table if exists {{ relation }}
{%- endmacro %}

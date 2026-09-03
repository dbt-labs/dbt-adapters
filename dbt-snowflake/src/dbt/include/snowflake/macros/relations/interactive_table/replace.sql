{% macro snowflake__get_replace_interactive_table_sql(relation, sql) -%}

    {%- set interactive_table = relation.from_config(config.model) -%}
    {{ snowflake__interactive_table_ddl_body_sql(interactive_table, relation, sql, ddl_prefix='create or replace interactive table') }}

{%- endmacro %}

{% macro snowflake__get_create_interactive_table_as_sql(relation, sql) -%}

    {%- set interactive_table = relation.from_config(config.model) -%}
    {{ snowflake__create_interactive_table_sql(interactive_table, relation, sql) }}

{%- endmacro %}


{% macro snowflake__create_interactive_table_sql(interactive_table, relation, sql) -%}
    {{ snowflake__interactive_table_ddl_body_sql(interactive_table, relation, sql, ddl_prefix='create interactive table') }}
{%- endmacro %}

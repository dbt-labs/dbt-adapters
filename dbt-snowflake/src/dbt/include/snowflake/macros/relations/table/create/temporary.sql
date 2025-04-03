{% macro snowflake__create_table_temporary_sql(relation, compiled_code) -%}
{#-
    Implements CREATE TEMPORARY TABLE and CREATE TEMPORARY TABLE ... AS SELECT:
    https://docs.snowflake.com/en/sql-reference/sql/create-table
    https://docs.snowflake.com/en/sql-reference/sql/create-table#create-table-as-select-also-referred-to-as-ctas
-#}

{%- set contract_config = config.get('contract') -%}
{%- if contract_config.enforced -%}
    {{- get_assert_columns_equivalent(compiled_code) -}}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
{%- endif -%}

{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

create or replace temporary table {{ relation }}
    {%- if contract_config.enforced %}
    {{ get_table_columns_and_constraints() }}
    {%- endif %}
as (
    {{ compiled_code }}
);

{%- endmacro %}

{% macro refresh_external_table(source_node) %}
    {{ return(adapter.dispatch('refresh_external_table', 'dbt')(source_node)) }}
{% endmacro %}

{% macro default__refresh_external_table(source_node) %}
    {% do return([]) %}
{% endmacro %}

{% macro update_external_table_columns(source_node) %}
    {{ return(adapter.dispatch('update_external_table_columns', 'dbt')(source_node)) }}
{% endmacro %}

{% macro default__update_external_table_columns(source_node) %}

{% endmacro %}

{%- macro create_external_schema(source_node) -%}
    {{ adapter.dispatch('create_external_schema', 'dbt')(source_node) }}
{%- endmacro -%}

{%- macro default__create_external_schema(source_node) -%}
    {%- set fqn -%}
        {%- if source_node.database -%}
            {{ source_node.database }}.{{ source_node.schema }}
        {%- else -%}
            {{ source_node.schema }}
        {%- endif -%}
    {%- endset -%}

    {%- set ddl -%}
        create schema if not exists {{ fqn }}
    {%- endset -%}

    {{ return(ddl) }}
{%- endmacro -%}


{% macro exit_transaction() %}
    {{ return(adapter.dispatch('exit_transaction', 'dbt')()) }}
{% endmacro %}

{% macro default__exit_transaction() %}
    {{ return('') }}
{% endmacro %}

{% macro dropif(node) %}
    {{ adapter.dispatch('dropif', 'dbt')(node) }}
{% endmacro %}

{% macro default__dropif() %}
    {{ exceptions.raise_compiler_error(
        "Dropping external tables is not implemented for the default adapter"
    ) }}
{% endmacro %}

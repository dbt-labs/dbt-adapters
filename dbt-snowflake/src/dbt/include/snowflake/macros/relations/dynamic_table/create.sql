{% macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}

    {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}
    {%- set dynamic_table = relation.from_config(config.model) -%}

    {%- if catalog_relation.catalog_type == 'NATIVE' -%}
        {{ snowflake__create_dynamic_table_standard_sql(dynamic_table, relation, compiled_code) }}
    {%- elif catalog_relation.catalog_type == 'ICEBERG_MANAGED' -%}
        {{ snowflake__create_dynamic_table_iceberg_managed_sql(dynamic_table, relation, compiled_code) }}
    {%- else -%}
        {% do exceptions.raise_compiler_error('Unexpected model config for: ' ~ relation) %}
    {%- endif -%}

{%- endmacro %}

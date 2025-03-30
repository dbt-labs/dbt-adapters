{% macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}

    {%- set catalog_integration = adapter.get_catalog_integration_from_model(config.model) -%}
    {%- set dynamic_table = relation.from_config(config.model) -%}

    {%- if catalog_integration is none -%}
        {{ snowflake__create_dynamic_table_standard_sql(dynamic_table, relation, compiled_code) }}
    {%- elif catalog_integration.catalog_type == 'iceberg_managed' -%}
        {{ snowflake__create_dynamic_table_iceberg_managed_sql(dynamic_table, relation, compiled_code) }}
    {%- endif -%}

{%- endmacro %}

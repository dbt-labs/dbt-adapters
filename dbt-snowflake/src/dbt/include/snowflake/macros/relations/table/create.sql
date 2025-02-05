{% macro snowflake__create_table_as(temporary, relation, compiled_code, language='sql') -%}

{%- if relation.is_iceberg_format and not adapter.behavior.enable_iceberg_materializations.no_warn -%}
    {%- do exceptions.raise_compiler_error('Was unable to create model as Iceberg Table Format. Please set the `enable_iceberg_materializations` behavior flag to True in your dbt_project.yml. For more information, go to https://docs.getdbt.com/reference/resource-configs/snowflake-configs#iceberg-table-format') -%}
{%- endif -%}

{%- set catalog_name = config.get('catalog_name', default=none) -%}
{%- if catalog_name is not none -%}
    {%- set catalog_integration = adapter.get_catalog_integration(catalog_name) -%}
{%- else -%}
    {%- set catalog_integration = none -%}
{%- endif -%}

{%- if language == 'sql' -%}
    {%- if catalog_integration is none -%}
        {{ snowflake__create_table_standard_sql(temporary, relation, compiled_code) }}
    {%- elif catalog_integration.catalog_type in ['iceberg_rest', 'aws_glue'] -%}
        {{ snowflake__create_table_iceberg_rest_sql(relation) }}
    {%- else -%}
        {%- do exceptions.raise_compiler_error("Unsupported catalog integration: " ~ catalog_name ~ " of type: " ~ catalog_integration.catalog_type) -%}
    {%- endif -%}
{%- elif language == 'python' -%}
    {%- if relation.is_iceberg_format -%}
        {%- do exceptions.raise_compiler_error('Iceberg is incompatible with Python models. Please use a SQL model for the iceberg format.') -%}
    {%- endif -%}
    {{ py_write_table(compiled_code=compiled_code, target_relation=relation, table_type=relation.get_ddl_prefix_for_create(config.model.config, temporary)) }}
{%- else -%}
    {%- do exceptions.raise_compiler_error("snowflake__create_table_as macro didn't get supported language, it got %s" % language) -%}
{%- endif -%}

{% endmacro %}

{% macro snowflake__create_table_as(temporary, relation, compiled_code, language='sql') -%}

    {%- set catalog_name = config.get('catalog_name', default=none) -%}
    {%- if catalog_name is not none -%}
        {%- set catalog_integration = adapter.get_catalog_integration(catalog_name) -%}
    {%- else -%}
        {%- set catalog_integration = none -%}
    {%- endif -%}

    {%- if language == 'sql' -%}
        {%- if catalog_integration is none -%}
            {{ snowflake__create_table_standard_sql(temporary, relation, compiled_code) }}
        {%- else -%}
            {%- do exceptions.raise_compiler_error("Unsupported catalog integration: " ~ catalog_name ~ " of type: " ~ catalog_integration.catalog_type) -%}
        {%- endif -%}

    {%- elif language == 'python' -%}
        {%- if relation.is_iceberg_format %}
            {% do exceptions.raise_compiler_error('Iceberg is incompatible with Python models. Please use a SQL model for the iceberg format.') %}
        {%- else -%}
            {{ py_write_table(compiled_code=compiled_code, target_relation=relation, table_type=relation.get_ddl_prefix_for_create(config.model.config, temporary)) }}
        {%- endif %}

    {%- else -%}
        {% do exceptions.raise_compiler_error("snowflake__create_table_as macro didn't get supported language, it got %s" % language) %}

    {%- endif -%}

{% endmacro %}

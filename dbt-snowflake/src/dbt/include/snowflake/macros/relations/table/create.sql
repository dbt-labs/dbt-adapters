{% macro snowflake__create_table_as(temporary, relation, compiled_code, language='sql') -%}

    {%- set catalog_name = model.config.get('catalog') -%}
    {%- set table_format = model.config.get('table_format') -%}

    {%- if catalog_name is not none -%}
        {%- set catalog_integration = adapter.get_catalog_integration(catalog_name) -%}
    {%- elif table_format == 'iceberg' -%}
        {%- do exceptions.warn("This configuration is deprecated. Please include `catalog: snowflake` in the future.") -%}
        {%- set catalog_integration = adapter.get_catalog_integration('snowflake') -%}
    {%- else -%}
        {%- set catalog_integration = none -%}
    {%- endif -%}

    {%- if language == 'sql' -%}
        {%- if temporary -%}
            {{ snowflake__create_table_temporary_sql(relation, compiled_code) }}
        {%- elif catalog_integration.catalog_type == 'iceberg_managed' -%}
            {{ snowflake__create_table_iceberg_managed_sql(relation, compiled_code) }}
        {%- else -%}
            {{ snowflake__create_table_standard_sql(relation, compiled_code) }}
        {%- endif -%}

    {%- elif language == 'python' -%}
        {%- if relation.is_iceberg_format %}
            {% do exceptions.raise_compiler_error('Iceberg is incompatible with Python models. Please use a SQL model for the iceberg format.') %}
        {%- else -%}
            {{ py_write_table(compiled_code, relation) }}
        {%- endif %}

    {%- else -%}
        {% do exceptions.raise_compiler_error("snowflake__create_table_as macro didn't get supported language, it got %s" % language) %}

    {%- endif -%}

{% endmacro %}

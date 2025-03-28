{% macro snowflake__create_table_as(temporary, relation, compiled_code, language='sql') -%}

    {%- set catalog_name = config.get('catalog') -%}
    {%- set table_format = config.get('table_format') -%}

    {%- if language == 'sql' -%}
        {%- if temporary -%}
            {{ snowflake__create_table_temporary_sql(relation, compiled_code) }}
        {%- elif catalog_name == 'snowflake' -%}
            {{ snowflake__create_table_iceberg_managed_sql(relation, compiled_code) }}
        {%- elif catalog_name is not none -%}
            {%- do exceptions.raise_compiler_error("Unsupported catalog integration: " ~ catalog_name ~ " of type: " ~ catalog_integration.catalog_type) -%}
        {%- elif table_format == 'iceberg' -%}
            {{ exceptions.warn("This configuration is deprecated. Please include `catalog: snowflake` in the future.") }}
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

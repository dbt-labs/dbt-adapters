{% macro snowflake__create_table_as(temporary, relation, compiled_code, language='sql') -%}

    {%- set catalog_integration = adapter.get_catalog_integration_from_model(config.model) -%}

    {%- if language == 'sql' -%}
        {%- if temporary -%}
            {{ snowflake__create_table_temporary_sql(relation, compiled_code) }}
        {%- elif catalog_integration is none -%}
            {{ snowflake__create_table_standard_sql(relation, compiled_code) }}
        {%- elif catalog_integration.catalog_type == 'iceberg_managed' -%}
            {{ snowflake__create_table_iceberg_managed_sql(relation, compiled_code) }}
        {%- else -%}
            {{ snowflake__create_table_standard_sql(relation, compiled_code) }}
        {%- endif -%}

    {%- elif language == 'python' -%}
        {%- if catalog_integration is not none %}
            {% do exceptions.raise_compiler_error('Iceberg is incompatible with Python models. Please use a SQL model for the iceberg format.') %}
        {%- else -%}
            {{ py_write_table(compiled_code, relation) }}
        {%- endif %}

    {%- else -%}
        {% do exceptions.raise_compiler_error("snowflake__create_table_as macro didn't get supported language, it got %s" % language) %}

    {%- endif -%}

{% endmacro %}

{% macro snowflake__create_table_as(temporary, relation, compiled_code, language='sql') -%}

    {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

    {%- if language == 'sql' -%}
        {%- if temporary -%}
            {{ snowflake__create_table_temporary_sql(relation, compiled_code) }}
        {%- elif catalog_relation.catalog_type == 'NATIVE' -%}
            {{ snowflake__create_table_standard_sql(relation, compiled_code) }}
        {%- elif catalog_relation.catalog_type == 'ICEBERG_MANAGED' -%}
            {{ snowflake__create_table_iceberg_managed_sql(relation, compiled_code) }}
        {%- else -%}
            {% do exceptions.raise_compiler_error('Unexpected model config for: ' ~ relation) %}
        {%- endif -%}

    {%- elif language == 'python' -%}
        {%- if catalog_relation.catalog_type == 'ICEBERG_MANAGED' %}
            {% do exceptions.raise_compiler_error('Iceberg is incompatible with Python models. Please use a SQL model for the iceberg format.') %}
        {%- else -%}
            {{ py_write_table(compiled_code, relation) }}
        {%- endif %}

    {%- else -%}
        {% do exceptions.raise_compiler_error("snowflake__create_table_as macro didn't get supported language, it got %s" % language) %}

    {%- endif -%}

{% endmacro %}

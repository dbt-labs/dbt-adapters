{% macro bigquery__generate_schema_name(custom_schema_name, node) -%}
    {%- set schema_name = custom_schema_name | trim if custom_schema_name is not none else none -%}
    {%- if schema_name is not none and '.' in schema_name -%}
        {#-- A dotted BigQuery schema is the literal `catalog.namespace` part of
             a Lakehouse PCNT name. It must not receive the usual target prefix. --#}
        {%- set parts = schema_name.split('.') -%}
        {%- if parts | length != 2
            or not parts[0]
            or not parts[1]
            or parts[0] != parts[0] | trim
            or parts[1] != parts[1] | trim -%}
            {%- do exceptions.raise_compiler_error(
                "Invalid BigQuery Lakehouse schema `" ~ schema_name ~ "`: expected "
                ~ "exactly `catalog.namespace` with both parts non-empty"
            ) -%}
        {%- endif -%}
        {{ schema_name }}
    {%- else -%}
        {{ default__generate_schema_name(custom_schema_name, node) }}
    {%- endif -%}
{%- endmacro %}

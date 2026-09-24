{% macro bigquery__snapshot_check_column_exists(column_name, existing_columns) -%}
    {# BigQuery resolves column references without regard to case, even when quoted. #}
    {{ return(column_name | lower in existing_columns | map('lower') | list) }}
{%- endmacro %}

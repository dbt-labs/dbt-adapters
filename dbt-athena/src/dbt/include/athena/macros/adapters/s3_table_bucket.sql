{# Centralized S3 Table Bucket detection with explicit config override.
   Users can set config(is_s3_table=true/false) to bypass auto-detection. #}
{% macro is_s3_table_bucket(database) -%}
  {%- set explicit = config.get('is_s3_table', none) -%}
  {%- if explicit is not none -%}
    {{ return(explicit | as_bool) }}
  {%- endif -%}
  {{ return(adapter.is_s3_table_bucket(database)) }}
{%- endmacro %}

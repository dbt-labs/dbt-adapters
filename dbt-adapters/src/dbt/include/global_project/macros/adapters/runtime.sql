{% macro is_core_v2() -%}
  {%- set major_version = (dbt_version.split('.') | first) | int -%}
  {{ return(major_version == 2) }}
{%- endmacro %}

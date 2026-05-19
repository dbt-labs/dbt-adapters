{% macro concat(fields) -%}
  {{ return(adapter.dispatch('concat', 'dbt')(fields)) }}
{%- endmacro %}

{% macro default__concat(fields) -%}
    {%- if fields | length == 1 -%}
        {{ fields[0] }}
    {%- else -%}
        {{ fields | join(' || ') }}
    {%- endif -%}
{%- endmacro %}

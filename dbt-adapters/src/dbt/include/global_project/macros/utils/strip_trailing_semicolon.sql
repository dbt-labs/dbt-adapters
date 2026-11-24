{% macro strip_trailing_semicolon(sql) -%}
  {{ return(adapter.dispatch('strip_trailing_semicolon', 'dbt')(sql)) }}
{%- endmacro %}

{% macro default__strip_trailing_semicolon(sql) -%}
  {%- if sql is none or not (sql is string) -%}
    {{ return(sql) }}
  {%- elif (sql.rstrip()).endswith(';') -%}
    {{ return((sql.rstrip()).rstrip('; \t\r\n')) }}
  {%- else -%}
    {{ return(sql) }}
  {%- endif -%}
{%- endmacro %}

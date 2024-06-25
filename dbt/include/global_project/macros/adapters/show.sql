{#
    We expect a syntax error if dbt show is invoked both with a --limit flag to show
    and with a limit predicate embedded in its inline query. No special handling is
    provided out-of-box.
#}
{% macro get_show_sql(compiled_code, sql_header, limit) -%}
  {{ adapter.dispatch('get_show_sql', 'dbt')(compiled_code, sql_header, limit) }}
{% endmacro %}

{% macro default__get_show_sql(compiled_code, sql_header, limit) %}
  {%- if sql_header is not none -%}
  {{ sql_header }}
  {%- endif -%}
  {{ compiled_code }}
  {%- if limit is not none %}
  limit {{ limit }}
  {%- endif -%}
{% endmacro %}

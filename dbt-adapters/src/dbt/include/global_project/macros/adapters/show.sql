{#
    We expect a syntax error if dbt show is invoked both with a --limit flag to show
    and with a limit predicate embedded in its inline query. No special handling is
    provided out-of-box.
#}
{% macro get_show_sql(compiled_code, sql_header, limit) -%}
  {%- if sql_header is not none -%}
  {{ sql_header }}
  {%- endif %}
  {{ get_limit_subquery_sql(compiled_code, limit) }}
{% endmacro %}

{#
    Not necessarily a true subquery anymore. Now, merely a query subordinate
    to the calling macro.
#}
{%- macro get_limit_subquery_sql(sql, limit) -%}
  {{ adapter.dispatch('get_limit_sql', 'dbt')(sql, limit) }}
{%- endmacro -%}

{% macro default__get_limit_sql(sql, limit) %}
  {{ compiled_code }}
  {% if limit is not none %}
  limit {{ limit }}
  {%- endif -%}
{% endmacro %}

{% macro create_schema(relation) -%}
  {{ adapter.dispatch('create_schema', 'dbt')(relation) }}
{% endmacro %}

{% macro default__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}


{% macro drop_schema(relation) -%}
  {{ adapter.dispatch('drop_schema', 'dbt')(relation) }}
{% endmacro %}

{% macro default__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }} cascade
  {% endcall %}
{% endmacro %}

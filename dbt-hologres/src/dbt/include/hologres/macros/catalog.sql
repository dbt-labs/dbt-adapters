{% macro hologres__get_catalog(information_schema, schemas) -%}
  {{ return(adapter.dispatch('get_catalog', 'dbt')(information_schema, schemas)) }}
{%- endmacro %}

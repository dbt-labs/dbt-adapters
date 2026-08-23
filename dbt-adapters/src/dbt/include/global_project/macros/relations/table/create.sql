{% macro get_create_table_as_sql(temporary, relation, sql) -%}
  {{ adapter.dispatch('get_create_table_as_sql', 'dbt')(temporary, relation, sql) }}
{%- endmacro %}

{% macro default__get_create_table_as_sql(temporary, relation, sql) -%}
  {% set plan = adapter.plan_create_from_query(temporary, relation, config.model) %}
  {{ return(render_create_from_query_plan(plan, relation, sql)) }}
{% endmacro %}


{% macro render_create_from_query_plan(plan, relation, sql) -%}
  {{ return(adapter.dispatch('render_create_from_query_plan', 'dbt')(plan, relation, sql)) }}
{% endmacro %}


{% macro default__render_create_from_query_plan(plan, relation, sql) -%}
  {% if plan.strategy == 'ctas' %}
    {{ return(create_table_as(plan.temporary, relation, sql)) }}
  {% elif plan.strategy == 'unsupported' %}
    {{ exceptions.raise_compiler_error(plan.reason) }}
  {% else %}
    {{ exceptions.raise_compiler_error(
      "Create-from-query strategy '" ~ plan.strategy ~ "' requires an adapter-specific renderer"
    ) }}
  {% endif %}
{% endmacro %}


/* {# keep logic under old macro name for backwards compatibility #} */
{% macro create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {# backward compatibility for create_table_as that does not support language #}
  {% if language == "sql" %}
    {{ adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code)}}
  {% else %}
    {{ adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code, language) }}
  {% endif %}

{%- endmacro %}

{% macro default__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced and (not temporary) %}
    {{ get_assert_columns_equivalent(sql) }}
    {{ get_table_columns_and_constraints() }}
    {%- set sql = get_select_subquery(sql) %}
  {% endif %}
  as (
    {{ sql }}
  );
{%- endmacro %}


{% macro default__get_column_names() %}
  {#- loop through user_provided_columns to get column names -#}
    {%- set user_provided_columns = model['columns'] -%}
    {%- for i in user_provided_columns %}
      {%- set col = user_provided_columns[i] -%}
      {%- set col_name = adapter.quote(col['name']) if col.get('quote') else col['name'] -%}
      {{ col_name }}{{ ", " if not loop.last }}
    {%- endfor -%}
{% endmacro %}


{% macro get_select_subquery(sql) %}
  {{ return(adapter.dispatch('get_select_subquery', 'dbt')(sql)) }}
{% endmacro %}

{% macro default__get_select_subquery(sql) %}
    select {{ adapter.dispatch('get_column_names', 'dbt')() }}
    from (
        {{ sql }}
    ) as model_subq
{%- endmacro %}

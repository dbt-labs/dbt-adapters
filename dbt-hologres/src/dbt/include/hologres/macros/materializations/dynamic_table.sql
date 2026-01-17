{% materialization dynamic_table, adapter='hologres' -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {% set grant_config = config.get('grants') %}

  {# 在这里读取配置 #}
  {%- set target_lag = config.require('target_lag') -%}
  {%- set auto_refresh_enable = config.get('auto_refresh_enable', true) -%}
  {%- set auto_refresh_mode = config.get('auto_refresh_mode', 'auto') -%}
  {%- set computing_resource = config.get('computing_resource', 'serverless') -%}
  {%- set orientation = config.get('orientation', 'column') -%}
  {%- set distribution_key = config.get('distribution_key', none) -%}
  {%- set clustering_key = config.get('clustering_key', none) -%}
  {%- set event_time_column = config.get('event_time_column', none) -%}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ hologres__get_create_dynamic_table_as_sql(False, intermediate_relation, sql, target_lag, auto_refresh_enable, auto_refresh_mode, computing_resource, orientation, distribution_key, clustering_key, event_time_column) }}
  {%- endcall %}

  -- cleanup
  {% if existing_relation is not none %}
    {% set existing_relation = load_cached_relation(existing_relation) %}
    {% if existing_relation is not none %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}

{% macro hologres__get_create_dynamic_table_as_sql(temporary, relation, sql, target_lag, auto_refresh_enable, auto_refresh_mode, computing_resource, orientation, distribution_key, clustering_key, event_time_column) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create dynamic table {{ relation }}
  with (
    freshness = '{{ target_lag }}',
    auto_refresh_enable = {{ 'true' if auto_refresh_enable else 'false' }},
    auto_refresh_mode = '{{ auto_refresh_mode }}',
    computing_resource = '{{ computing_resource }}',
    orientation = '{{ orientation }}'
    {%- if distribution_key is not none %},
    distribution_key = '{{ distribution_key }}'
    {%- endif %}
    {%- if clustering_key is not none %},
    clustering_key = '{{ clustering_key }}'
    {%- endif %}
    {%- if event_time_column is not none %},
    event_time_column = '{{ event_time_column }}'
    {%- endif %}
  )
  as
  {{ sql }};
{%- endmacro %}

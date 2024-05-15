{% macro mat_external_table(source_node) %}
    {{ adapter.dispatch('mat_external_table', 'dbt')(source_node) }}
{% endmacro %}

{% macro default__mat_external_table(source_node) %}

  {%- set full_refresh_mode = (should_full_refresh()) -%}


  {%- set old_relation = adapter.get_relation(database=source_node.database, schema=source_node.schema, identifier=source_node.name) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
  {%- set exists_as_external_table = (old_relation is not none and old_relation.is_external) -%}

  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {# {% set build_plan = "" %} #}
  {% set build_plan = create_external_table(source_node) %}

  {% set create_or_replace = (old_relation is none or full_refresh_mode) %}
  {{ log('create_or_replace: ' ~ create_or_replace, info=True) }}

  {# {% if exists_as_view %}
    {{ exceptions.raise_compiler_error("Cannot make ExTab to '{}', it is already view".format(old_relation)) }}
   {% elif exists_as_table %}
    {{ exceptions.raise_compiler_error("Cannot make ExTab '{}', it is a already a table".format(old_relation)) }}
  {% elif exists_as_external_table %}
    {% set build_plan = build_plan + refresh_external_table(source_node) %}
  {% else %}
    {% set build_plan = build_plan + [
      create_external_schema(source_node),
      create_external_table(source_node)
    ] %}
  {% endif %} #}

  {% set code = 'CREATE' if create_or_replace else 'REFRESH' %}

  {{ log('XXX: build_plan: ' ~ build_plan, info=True) }}
  {% do run_query(build_plan) %}


  {% set target_relation = old_relation.incorporate(type='external_table') %}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmacro %}

{% materialization external_table, default %}

  {{ mat_external_table(source_node) }}


{% endmaterialization %}

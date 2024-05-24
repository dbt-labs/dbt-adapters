{% materialization external_table, default %}

  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type=this.ExternalTable) %}

  {{ log('existing_relation: ' ~ existing_relation, info=True) }}
  {{ log('existing_relation.type: ' ~ existing_relation.type, info=True) }}

  {%- set exists_as_table = (existing_relation is not none and existing_relation.is_table) -%}
  {%- set exists_as_view = (existing_relation is not none and existing_relation.is_view) -%}
  {%- set exists_as_external_table = (existing_relation is not none and existing_relation.is_external_table) -%}

  -- build model
  {% set build_plan = [] %}
  {% set code = 'CREATE' %}

  {% if exists_as_view %}
      {% set build_plan = build_plan + [
      drop_relation_if_exists(existing_relation),
      create_external_table(target_relation, model.columns.values())
    ] %}
   {% elif exists_as_table %}
    {% if full_refresh_mode %}
      {% set build_plan = build_plan + [create_external_table(target_relation, model.columns.values())] %}
    {% elif not full_refresh_mode %} 
      {% set code = 'REFRESH' %}
      {% set build_plan = build_plan + refresh_external_table(target_relation) %}
    {% endif %}
  {% else %}
    {% set build_plan = build_plan + [
      create_external_table(target_relation, model.columns.values())
    ] %}
  {% endif %}

  {% call noop_statement('main', code, code) %}
    {{ build_plan }};
  {% endcall %}

  {%- set grant_config = config.get('grants') -%}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

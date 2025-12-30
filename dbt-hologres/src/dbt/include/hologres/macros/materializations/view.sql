{%- materialization view, adapter='hologres' -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}

  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'view' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model (Hologres需要在事务外执行CREATE VIEW)
  -- 由于Hologres的add_begin_query被禁用，实际上不会开启事务
  {% call statement('main') -%}
    {{ get_create_view_as_sql(intermediate_relation, sql) }}
  {%- endcall %}

  -- cleanup
  {#- Drop the existing view if it exists instead of renaming to backup -#}
  {#- This avoids issues with Hologres ALTER VIEW RENAME when schema is empty string -#}
  {% if existing_relation is not none %}
    {#- Re-check if the relation actually exists in the database before attempting to drop it -#}
    {% set existing_relation = adapter.get_relation(database=existing_relation.database, schema=existing_relation.schema, identifier=existing_relation.identifier) %}
    {% if existing_relation is not none %}
        {{ drop_relation_if_exists(existing_relation) }}
    {% endif %}
  {% endif %}
  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}

{% materialization seed, default %}

  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if old_relation is none else old_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {%- set can_use_relation_rename = seed_can_use_relation_rename() -%}

  {%- set grant_config = config.get('grants') -%}
  {%- set agate_table = load_agate_table() -%}
  -- grab current tables grants config for comparison later on

  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% set create_table_sql = "" %}
  {% if exists_as_view %}
    {{ exceptions.raise_compiler_error("Cannot seed to '{}', it is a view".format(old_relation.render())) }}
  {% elif exists_as_table %}
    {% set create_table_sql = reset_csv_table(model, full_refresh_mode, old_relation, agate_table, intermediate_relation) %}
  {% else %}
    {% set create_table_sql = create_csv_table(model, agate_table, intermediate_relation) %}
  {% endif %}

  {% set code = 'CREATE' if full_refresh_mode else 'INSERT' %}
  {% set rows_affected = (agate_table.rows | length) %}
  {% set sql = load_csv_rows(model, agate_table, intermediate_relation) %}

  {% call noop_statement('main', code ~ ' ' ~ rows_affected, code, rows_affected) %}
    {{ get_csv_sql(create_table_sql, sql) }};
  {% endcall %}

  {% do create_indexes(intermediate_relation) %}

  -- cleanup
  {% if exists_as_table and not full_refresh_mode %}
      -- For non-full refresh, doing an atomic insert so we don't drop grants
      {% call statement('truncate_target') -%}
          {{ truncate_relation(target_relation) }}
      {%- endcall %}
      {% call statement('insert_to_target') -%}
          insert into {{ target_relation.render() }}
          select * from {{ intermediate_relation.render() }}
      {%- endcall %}
      -- drop the intermediate relation since we just inserted its rows
      {{ adapter.drop_relation(intermediate_relation) }}
  {% elif can_use_relation_rename %}
      {% if old_relation is not none %}
          {{ adapter.rename_relation(old_relation, backup_relation) }}
      {% endif %}
      {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {% else %}
      {% if old_relation is not none %}
          {{ adapter.drop_relation(old_relation) }}
      {% endif %}
      {% call statement('create_target_from_intermediate') -%}
          create table {{ target_relation.render() }} as
          select * from {{ intermediate_relation.render() }}
      {%- endcall %}
      {{ adapter.drop_relation(intermediate_relation) }}
  {% endif %}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {% if full_refresh_mode or not exists_as_table %}
    {{ drop_relation_if_exists(backup_relation) }}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

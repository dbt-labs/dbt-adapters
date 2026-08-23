
{% materialization incremental, default -%}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set language = model['language'] -%}
  {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}
  {%- set incremental_strategy = config.get('incremental_strategy') or 'default' -%}
  {%- set incremental_plan = adapter.plan_incremental_mutation(
      incremental_strategy,
      language=language,
      unique_key=unique_key,
      requested_temp_relation_type=config.get('tmp_relation_type'),
      catalog_relation=catalog_relation
  ) -%}
  {%- set strategy_sql_macro_func = adapter.get_incremental_plan_macro(context, incremental_plan) -%}
  {%- set temp_relation = make_temp_relation(target_relation) -%}
  {%- if incremental_plan.temp_relation_type is not none -%}
    {%- set temp_relation_object_type = 'table' if incremental_plan.temp_relation_type.value == 'transient' else incremental_plan.temp_relation_type.value -%}
    {%- set temp_relation = temp_relation.incorporate(type=temp_relation_object_type) -%}
  {%- endif -%}
  {%- set staging_is_temporary = incremental_plan.catalog_staging.value != 'permanent_table_only' -%}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {% if existing_relation is none %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
      {% set relation_for_indexes = target_relation %}
  {% elif full_refresh_mode %}
      {% set build_sql = get_create_table_as_sql(False, intermediate_relation, sql) %}
      {% set relation_for_indexes = intermediate_relation %}
      {% set need_swap = true %}
  {% else %}
    {% do run_query(get_create_table_as_sql(staging_is_temporary, temp_relation, sql)) %}
    {% set relation_for_indexes = temp_relation %}
    {% set contract_config = config.get('contract') %}
    {% if not contract_config or not contract_config.enforced %}
      {% do adapter.expand_target_column_types(
               from_relation=temp_relation,
               to_relation=target_relation) %}
    {% endif %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_args = adapter.plan_incremental_arguments(
        target_relation=target_relation,
        temp_relation=temp_relation,
        unique_key=unique_key,
        dest_columns=dest_columns,
        incremental_predicates=incremental_predicates,
        adapter_arguments={'catalog_relation': catalog_relation, 'incremental_plan': incremental_plan}
    ) %}
    {% set build_sql = strategy_sql_macro_func(strategy_args.to_macro_dict()) %}

  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(relation_for_indexes) %}
  {% endif %}

  {% if need_swap %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

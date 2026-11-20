{#-
  One statement per run, with no temporary relation: `append` on an existing table sends
  `insert into ... by name`; anything else (a first run, `--full-refresh`, `insert_overwrite`) is
  the table materialization's `create or replace table`.
-#}
{% materialization incremental, adapter='lakecompute' %}

  {#- before the existence lookup, so a refused model sends nothing -#}
  {%- set strategy = config.get('incremental_strategy') or 'append' -%}
  {%- if strategy not in adapter.valid_incremental_strategies() -%}
    {{ exceptions.raise_compiler_error(
      "lakecompute supports the incremental strategies 'append' and 'insert_overwrite'; model "
      ~ model.unique_id ~ " uses '" ~ strategy ~ "'."
    ) }}
  {%- endif -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set grant_config = config.get('grants') -%}

  {#- exactly when is_incremental() is true, so the model's filter and the statement agree -#}
  {%- set append = (
    strategy == 'append'
    and existing_relation is not none
    and existing_relation.is_table
    and not should_full_refresh()
  ) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set drops = [] -%}
  {%- if append -%}
    {#- before any ALTER, as the default checks its temp table before changing the schema -#}
    {%- if config.get('contract').enforced -%}
      {%- do get_assert_columns_equivalent(sql) -%}
    {%- endif -%}
    {%- if on_schema_change != 'ignore' -%}
      {%- set drops = lakecompute__process_schema_changes(on_schema_change, existing_relation, sql) -%}
    {%- endif -%}
  {%- endif %}

  {% call statement('main') -%}
    {%- if append -%}
      {{ lakecompute__get_insert_by_name_sql(target_relation, sql) }}
    {%- else -%}
      {{ get_create_table_as_sql(False, target_relation, sql) }}
    {%- endif -%}
  {%- endcall %}

  {#- after the append, whose incremental filter may still read a dropped column from {{ this }} -#}
  {% do lakecompute__alter_columns(existing_relation, drops, on_schema_change) %}

  {% if not append %}
    {% do lakecompute__cache_replaced(existing_relation, target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=not append) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{#-
  Shadows dbt's `is_incremental()`: `insert_overwrite` replaces the whole table, so its query must
  select everything. The config checks come first, as the lookup can cost a `describe`.
-#}
{% macro is_incremental() %}
  {#-- do not run introspective queries in parsing #}
  {% if not execute
        or model.config.materialized != 'incremental'
        or should_full_refresh()
        or config.get('incremental_strategy') == 'insert_overwrite' %}
    {{ return(False) }}
  {% endif %}
  {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
  {{ return(relation is not none and relation.type == 'table') }}
{% endmacro %}


{#-
  dbt's `process_schema_changes` without a temp table: `describe` the query and the table in one
  request. A type is never changed in place. Fails before any ALTER, adds new columns, and returns
  the drops for the materialization to send after the append.
-#}
{% macro lakecompute__process_schema_changes(on_schema_change, relation, compiled_code) %}
  {#- nothing persists between requests, so the query needs its header -#}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set diff_sql -%}
    {{ sql_header if sql_header is not none }}
    select
        coalesce(s.column_name, t.column_name) as column_name,
        s.column_type as model_type,
        t.column_type as table_type
    from (describe select * from (
{{ compiled_code }}
    ) as __dbt_sbq) as s
    full outer join (describe {{ relation.render() }}) as t
        on lower(s.column_name) = lower(t.column_name)
    where s.column_name is null or t.column_name is null or s.column_type != t.column_type
    order by 1
  {%- endset -%}

  {%- set added = [] -%}
  {%- set removed = [] -%}
  {%- set retyped = [] -%}
  {%- for row in run_query(diff_sql) -%}
    {%- if row['table_type'] is none -%}
      {%- do added.append(row) -%}
    {%- elif row['model_type'] is none -%}
      {%- do removed.append(row) -%}
    {%- else -%}
      {%- do retyped.append(row) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set refused = [] -%}
  {%- if on_schema_change == 'fail' -%}
    {%- for row in added -%}
      {%- do refused.append(row['column_name'] ~ ': not in the table') -%}
    {%- endfor -%}
    {%- for row in removed -%}
      {%- do refused.append(row['column_name'] ~ ': no longer selected by the model') -%}
    {%- endfor -%}
  {%- endif -%}
  {#- with a contract, append_new_columns would silently cast to the table's old type -#}
  {%- if on_schema_change in ['fail', 'sync_all_columns'] or config.get('contract').enforced -%}
    {%- for row in retyped -%}
      {%- do refused.append(row['column_name'] ~ ': ' ~ row['table_type'] ~ ' in the table, '
                            ~ row['model_type'] ~ ' in the model') -%}
    {%- endfor -%}
  {%- endif -%}

  {%- if refused -%}
    {%- set reason = "" if on_schema_change == 'fail'
          else ", and lakecompute cannot change a column's type in place" -%}
    {{ exceptions.raise_compiler_error(
      "The columns of incremental model " ~ model.unique_id ~ " no longer match "
      ~ relation.render() ~ reason ~ " (on_schema_change='" ~ on_schema_change ~ "'):\n  - "
      ~ refused | sort | join("\n  - ")
      ~ "\nRe-run with --full-refresh to rebuild the table."
    ) }}
  {%- endif -%}

  {%- set adds = [] -%}
  {%- for row in added -%}
    {%- do adds.append('alter table ' ~ relation.render() ~ ' add column '
                       ~ adapter.quote(row['column_name']) ~ ' ' ~ row['model_type']) -%}
  {%- endfor -%}
  {% do lakecompute__alter_columns(relation, adds, on_schema_change) %}

  {%- set drops = [] -%}
  {%- if on_schema_change == 'sync_all_columns' -%}
    {%- for row in removed -%}
      {%- do drops.append('alter table ' ~ relation.render() ~ ' drop column '
                          ~ adapter.quote(row['column_name'])) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(drops) }}
{% endmacro %}


{#- the service takes one ALTER per statement, so they are sent `;`-joined, as one request -#}
{% macro lakecompute__alter_columns(relation, statements, on_schema_change) %}
  {%- if statements -%}
    {% call statement('sync_column_schemas') -%}
      {{ statements | join(';\n') }}
    {%- endcall %}
    {% do log("In " ~ relation.render() ~ " (on_schema_change='" ~ on_schema_change ~ "'): "
              ~ statements | join('; ')) %}
  {%- endif -%}
{% endmacro %}


{% macro lakecompute__get_insert_by_name_sql(relation, compiled_code) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

insert into {{ relation.render() }} by name (
{{ compiled_code }}
)
{%- endmacro %}

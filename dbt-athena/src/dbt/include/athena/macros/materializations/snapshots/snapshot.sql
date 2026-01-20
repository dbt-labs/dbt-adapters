{#
  Hash function to generate dbt_scd_id. dbt by default uses md5(coalesce(cast(field as varchar)))
   but it does not work with athena. It throws an error Unexpected parameters (varchar) for function
   md5. Expected: md5(varbinary)
#}
{% macro athena__snapshot_hash_arguments(args) -%}
    to_hex(md5(to_utf8({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})))
{%- endmacro %}


{#
    If hive table then Recreate the snapshot table from the new_snapshot_table
    If iceberg table then Update the standard snapshot merge to include the DBT_INTERNAL_SOURCE prefix in the src_cols_csv
#}
{% macro hive_snapshot_merge_sql(target, source, insert_cols, table_type) -%}
    {%- set target_relation = adapter.get_relation(database=target.database, schema=target.schema, identifier=target.identifier) -%}
    {%- if target_relation is not none -%}
      {% do adapter.drop_relation(target_relation) %}
    {%- endif -%}

    {% set sql -%}
      select * from {{ source }};
    {%- endset -%}

    {{ create_table_as(False, target_relation, sql) }}
{% endmacro %}

{% macro iceberg_snapshot_merge_sql(target, source, insert_cols) %}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}
    {%- set src_columns = [] -%}
    {%- for col in insert_cols -%}
      {%- do src_columns.append('dbt_internal_source.' + col) -%}
    {%- endfor -%}
    {%- set src_cols_csv = src_columns | join(', ') -%}

    merge into {{ target }} as dbt_internal_dest
    using {{ source }} as dbt_internal_source
    on dbt_internal_source.dbt_scd_id = dbt_internal_dest.dbt_scd_id

    when matched
     and dbt_internal_dest.dbt_valid_to is null
     and dbt_internal_source.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = dbt_internal_source.dbt_valid_to

    when not matched
     and dbt_internal_source.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ src_cols_csv }})
{% endmacro %}


{# Athena-specific ISO8601 timestamp for backfill audit column #}
{% macro athena__snapshot_backfill_timestamp() %}
    {{ return("format_datetime(current_timestamp, 'yyyy-MM-dd''T''HH:mm:ss''Z''')") }}
{% endmacro %}


{# Athena-specific JSON entries builder #}
{% macro athena__backfill_audit_json_entries(columns) %}
    {%- set entries = [] -%}
    {%- for col in columns -%}
        {%- do entries.append("'\"" ~ col.name ~ "\": \"' || " ~ snapshot_backfill_timestamp() ~ " || '\"'") -%}
    {%- endfor -%}
    {{ return("concat(" ~ entries | join(", ', ', ") ~ ")") }}
{% endmacro %}


{# Athena backfill for Iceberg tables using MERGE syntax #}
{% macro athena__backfill_snapshot_columns(relation, columns, source_sql, unique_key, audit_column) %}
    {%- set column_names = columns | map(attribute='name') | list -%}
    {%- set table_type = config.get('table_type', 'hive') -%}
    
    {% if table_type != 'iceberg' %}
        {{ log("WARNING: Snapshot backfill for Athena requires 'table_type: iceberg'. Backfill skipped for hive table '" ~ relation.identifier ~ "'.", info=true) }}
        {{ return('') }}
    {% endif %}
    
    {% call statement('backfill_snapshot_columns') %}
    MERGE INTO {{ relation }} AS dbt_backfill_target
    USING ({{ source_sql }}) AS dbt_backfill_source
    ON {{ backfill_unique_key_join(unique_key, 'dbt_backfill_target', 'dbt_backfill_source') }}
    WHEN MATCHED THEN UPDATE SET
        {%- for col in columns %}
        {{ adapter.quote(col.name) }} = dbt_backfill_source.{{ adapter.quote(col.name) }}
        {%- if not loop.last or audit_column %},{% endif %}
        {%- endfor %}
        {%- if audit_column %}
        {{ adapter.quote(audit_column) }} = CASE 
            WHEN dbt_backfill_target.{{ adapter.quote(audit_column) }} IS NULL THEN 
                concat('{', {{ backfill_audit_json_entries(columns) }}, '}')
            ELSE 
                concat(
                    substr(dbt_backfill_target.{{ adapter.quote(audit_column) }}, 1, length(dbt_backfill_target.{{ adapter.quote(audit_column) }}) - 1),
                    ', ',
                    {{ backfill_audit_json_entries(columns) }},
                    '}'
                )
        END
        {%- endif %}
    {% endcall %}
    
    {{ log("WARNING: Backfilling " ~ columns | length ~ " new column(s) [" ~ column_names | join(', ') ~ "] in snapshot '" ~ relation.identifier ~ "'. Historical rows will be populated with CURRENT source values, not point-in-time historical values.", info=true) }}
{% endmacro %}


{# Athena-specific ensure audit column exists #}
{% macro athena__ensure_backfill_audit_column(relation, audit_column) %}
    {%- set table_type = config.get('table_type', 'hive') -%}
    {%- set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    
    {%- if audit_column | lower not in existing_columns -%}
        {% if table_type == 'iceberg' %}
            {% call statement('add_backfill_audit_column') %}
                ALTER TABLE {{ relation }} ADD COLUMNS ({{ adapter.quote(audit_column) }} VARCHAR);
            {% endcall %}
            {{ log("Added backfill audit column '" ~ audit_column ~ "' to snapshot '" ~ relation.identifier ~ "'.", info=true) }}
        {% else %}
            {{ log("WARNING: Cannot add audit column to hive table. Backfill audit will not be tracked.", info=true) }}
        {% endif %}
    {%- endif -%}
{% endmacro %}


{#
    Create a new temporary table that will hold the new snapshot results.
    This table will then be used to overwrite the target snapshot table.
#}
{% macro hive_create_new_snapshot_table(target, source, insert_cols) %}
    {%- set temp_relation = make_temp_relation(target, '__dbt_tmp_snapshot') -%}
    {%- set preexisting_tmp_relation = load_cached_relation(temp_relation) -%}
    {%- if preexisting_tmp_relation is not none -%}
      {%- do adapter.drop_relation(preexisting_tmp_relation) -%}
    {%- endif -%}

    {# TODO: Add insert_cols #}
    {%- set src_columns = [] -%}
    {%- set dst_columns = [] -%}
    {%- set updated_columns = [] -%}
    {%- for col in insert_cols -%}
      {%- do src_columns.append('dbt_internal_source.' + col) -%}
      {%- do dst_columns.append('dbt_internal_dest.' + col) -%}
      {%- if col.replace('"', '') in ['dbt_valid_to'] -%}
        {%- do updated_columns.append('dbt_internal_source.' + col) -%}
      {%- else -%}
        {%- do updated_columns.append('dbt_internal_dest.' + col) -%}
      {%- endif -%}
    {%- endfor -%}
    {%- set src_cols_csv = src_columns | join(', ') -%}
    {%- set dst_cols_csv = dst_columns | join(', ') -%}
    {%- set updated_cols_csv = updated_columns | join(', ') -%}

    {%- set source_columns = adapter.get_columns_in_relation(source) -%}

    {% set sql -%}
      -- Unchanged rows
      select {{ dst_cols_csv }}
      from {{ target }} as dbt_internal_dest
      left join {{ source }} as dbt_internal_source
      on dbt_internal_source.dbt_scd_id = dbt_internal_dest.dbt_scd_id
      where dbt_internal_source.dbt_scd_id is null

      union all

      -- Updated or deleted rows
      select {{ updated_cols_csv }}
      from {{ target }} as dbt_internal_dest
      inner join {{ source }} as dbt_internal_source
      on dbt_internal_source.dbt_scd_id = dbt_internal_dest.dbt_scd_id
      where dbt_internal_dest.dbt_valid_to is null
        and dbt_internal_source.dbt_change_type in ('update', 'delete')

      union all

      -- New rows
      select {{ src_cols_csv }}
      from {{ source }} as dbt_internal_source
      left join {{ target }} as dbt_internal_dest
      on dbt_internal_dest.dbt_scd_id = dbt_internal_source.dbt_scd_id
      where dbt_internal_dest.dbt_scd_id is null

    {%- endset -%}

    {% call statement('create_new_snapshot_table') %}
        {{ create_table_as(False, temp_relation, sql) }}
    {% endcall %}

    {% do return(temp_relation) %}
{% endmacro %}

{% materialization snapshot, adapter='athena' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}
  {%- set strategy_name = config.get('strategy') -%}
  {%- set file_format = config.get('file_format', 'parquet') -%}
  {%- set table_type = config.get('table_type', 'hive') -%}

  {%- set lf_tags_config = config.get('lf_tags_config') -%}
  {%- set lf_grants = config.get('lf_grants') -%}

  {{ log('Checking if target table exists') }}
  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_relation = make_temp_relation(target_relation) %}
      {%- set preexisting_staging_relation = load_cached_relation(staging_relation) -%}
      {%- if preexisting_staging_relation is not none -%}
      {%- do adapter.drop_relation(preexisting_staging_relation) -%}
      {%- endif -%}

      {% set staging_table = build_snapshot_staging_table(strategy, sql, target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}


      {% if missing_columns %}
        {% do alter_relation_add_columns(target_relation, missing_columns, table_type) %}
      {% endif %}

      {# Snapshot Column Backfill: Optionally backfill historical rows with current source values #}
      {# Note: Only supported for Iceberg tables in Athena #}
      {% set unique_key = config.get('unique_key') %}
      {% if missing_columns | length > 0 and snapshot_backfill_enabled() %}
          {% set audit_column = get_backfill_audit_column() %}
          
          {# Add audit column if configured and doesn't exist #}
          {% if audit_column %}
              {% do ensure_backfill_audit_column(target_relation, audit_column) %}
          {% endif %}
          
          {# Backfill historical rows with current source values #}
          {% do backfill_snapshot_columns(
              target_relation, 
              missing_columns, 
              model['compiled_sql'], 
              unique_key,
              audit_column
          ) %}
      {% endif %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% if table_type == 'iceberg' %}
          {% set final_sql = iceberg_snapshot_merge_sql(
                target = target_relation,
                source = staging_table,
                insert_cols = quoted_source_columns,
             )
          %}
      {% else %}
          {% set new_snapshot_table = hive_create_new_snapshot_table(
                target = target_relation,
                source = staging_table,
                insert_cols = quoted_source_columns,
             )
          %}
          {% set final_sql = hive_snapshot_merge_sql(
                target = target_relation,
                source = new_snapshot_table
             )
          %}
      {% endif %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% if staging_table is defined %}
      {% do adapter.drop_relation(staging_table) %}
  {% endif %}

  {% if new_snapshot_table is defined %}
      {% do adapter.drop_relation(new_snapshot_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% if lf_tags_config is not none %}
    {{ adapter.add_lf_tags(target_relation, lf_tags_config) }}
  {% endif %}

  {% if lf_grants is not none %}
    {{ adapter.apply_lf_grants(target_relation, lf_grants) }}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

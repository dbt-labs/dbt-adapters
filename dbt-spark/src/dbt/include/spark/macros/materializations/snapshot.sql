{% macro spark__snapshot_hash_arguments(args) -%}
    md5({%- for arg in args -%}
        coalesce(cast({{ arg }} as string ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}


{% macro spark__snapshot_string_as_time(timestamp) -%}
    {%- set result = "to_timestamp('" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}


{% macro spark__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    {% if target.is_iceberg %}
      {# create view only supports a name (no catalog, or schema) #}
      using {{ source.identifier }} as DBT_INTERNAL_SOURCE
    {% else %}
      using {{ source }} as DBT_INTERNAL_SOURCE
    {% endif %}
    on DBT_INTERNAL_SOURCE.{{ columns.dbt_scd_id }} = DBT_INTERNAL_DEST.{{ columns.dbt_scd_id }}
    when matched
     {% if config.get("dbt_valid_to_current") %}
       and ( DBT_INTERNAL_DEST.{{ columns.dbt_valid_to }} = {{ config.get('dbt_valid_to_current') }} or
             DBT_INTERNAL_DEST.{{ columns.dbt_valid_to }} is null )
     {% else %}
       and DBT_INTERNAL_DEST.{{ columns.dbt_valid_to }} is null
     {% endif %}
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set {{ columns.dbt_valid_to }} = DBT_INTERNAL_SOURCE.{{ columns.dbt_valid_to }}

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert *
    ;
{% endmacro %}


{% macro spark_build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set tmp_identifier = target_relation.identifier ~ '__dbt_tmp' %}

    {% if target_relation.is_iceberg %}
      {# iceberg catalog does not support create view, but regular spark does. We removed the catalog and schema #}
      {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                                    schema=none,
                                                    database=none,
                                                    type='view') -%}
    {% else %}
      {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                                    schema=target_relation.schema,
                                                    database=none,
                                                    type='view') -%}
    {% endif %}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {# needs to be a non-temp view so that its columns can be ascertained via `describe` #}
    {% call statement('build_snapshot_staging_relation') %}
        {{ create_view_as(tmp_relation, select) }}
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}


{% macro spark__post_snapshot(staging_relation) %}
    {% do adapter.drop_relation(staging_relation) %}
{% endmacro %}


{# Spark-specific ISO8601 timestamp for backfill audit column #}
{% macro spark__snapshot_backfill_timestamp() %}
    {{ return("date_format(current_timestamp(), \"yyyy-MM-dd'T'HH:mm:ss'Z'\")") }}
{% endmacro %}


{# Spark-specific JSON entries builder #}
{% macro spark__backfill_audit_json_entries(columns) %}
    {%- set entries = [] -%}
    {%- for col in columns -%}
        {%- do entries.append("'\"" ~ col.name ~ "\": \"', " ~ snapshot_backfill_timestamp() ~ ", '\"'") -%}
    {%- endfor -%}
    {{ return("concat(" ~ (entries | join(", ', ', ")) ~ ")") }}
{% endmacro %}


{# Spark backfill using MERGE syntax #}
{% macro spark__backfill_snapshot_columns(relation, columns, source_sql, unique_key, audit_column) %}
    {%- set column_names = columns | map(attribute='name') | list -%}

    {% call statement('backfill_snapshot_columns') %}
    MERGE INTO {{ relation }} AS dbt_backfill_target
    USING ({{ source_sql }}) AS dbt_backfill_source
    ON {{ spark__backfill_unique_key_join(unique_key, 'dbt_backfill_target', 'dbt_backfill_source') }}
    WHEN MATCHED THEN UPDATE SET
        {%- for col in columns %}
        dbt_backfill_target.`{{ col.name }}` = dbt_backfill_source.`{{ col.name }}`
        {%- if not loop.last or audit_column %},{% endif %}
        {%- endfor %}
        {%- if audit_column %}
        dbt_backfill_target.`{{ audit_column }}` = CASE
            WHEN dbt_backfill_target.`{{ audit_column }}` IS NULL THEN
                concat('{', {{ backfill_audit_json_entries(columns) }}, '}')
            ELSE
                concat(
                    substring(dbt_backfill_target.`{{ audit_column }}`, 1, length(dbt_backfill_target.`{{ audit_column }}`) - 1),
                    ', ',
                    {{ backfill_audit_json_entries(columns) }},
                    '}'
                )
        END
        {%- endif %}
    {% endcall %}

    {{ log("WARNING: Backfilling " ~ columns | length ~ " new column(s) [" ~ column_names | join(', ') ~ "] in snapshot '" ~ relation.identifier ~ "'. Historical rows will be populated with CURRENT source values, not point-in-time historical values.", info=true) }}
{% endmacro %}


{# Spark-specific unique key join - uses backticks consistently #}
{% macro spark__backfill_unique_key_join(unique_key, target_alias, source_alias) %}
    {% if unique_key | is_list %}
        {% for key in unique_key %}
            {{ target_alias }}.`{{ key }}` = {{ source_alias }}.`{{ key }}`
            {%- if not loop.last %} AND {% endif %}
        {% endfor %}
    {% else %}
        {{ target_alias }}.`{{ unique_key }}` = {{ source_alias }}.`{{ unique_key }}`
    {% endif %}
{% endmacro %}


{# Spark-specific ensure audit column exists #}
{% macro spark__ensure_backfill_audit_column(relation, audit_column) %}
    {%- set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {%- if audit_column | lower not in existing_columns -%}
        {% call statement('add_backfill_audit_column') %}
            ALTER TABLE {{ relation }} ADD COLUMNS (`{{ audit_column }}` STRING);
        {% endcall %}
        {{ log("Added backfill audit column '" ~ audit_column ~ "' to snapshot '" ~ relation.identifier ~ "'.", info=true) }}
    {%- endif -%}
{% endmacro %}


{% macro spark__create_columns(relation, columns) %}
    {% if columns|length > 0 %}
    {% call statement() %}
      alter table {{ relation }} add columns (
        {% for column in columns %}
          `{{ column.name }}` {{ column.data_type }} {{- ',' if not loop.last -}}
        {% endfor %}
      );
    {% endcall %}
    {% endif %}
{% endmacro %}


{% materialization snapshot, adapter='spark' %}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}
  {%- set file_format = config.get('file_format') or 'parquet' -%}
  {%- set grant_config = config.get('grants') -%}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=none,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

  {%- if file_format not in ['delta', 'iceberg', 'hudi'] -%}
    {% set invalid_format_msg -%}
      Invalid file format: {{ file_format }}
      Snapshot functionality requires file_format be set to 'delta' or 'iceberg' or 'hudi'
    {%- endset %}
    {% do exceptions.raise_compiler_error(invalid_format_msg) %}
  {% endif %}

  {%- if target_relation_exists -%}
    {%- if not target_relation.is_delta and not target_relation.is_iceberg and not target_relation.is_hudi -%}
      {% set invalid_format_msg -%}
        The existing table {{ model.schema }}.{{ target_table }} is in another format than 'delta' or 'iceberg' or 'hudi'
      {%- endset %}
      {% do exceptions.raise_compiler_error(invalid_format_msg) %}
    {% endif %}
  {% endif %}

  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.schema) %}
  {% endif %}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", model['config'], target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_code']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

      {% set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() %}

      {{ adapter.valid_snapshot_target(target_relation, columns) }}

      {% set staging_table = spark_build_snapshot_staging_table(strategy, sql, target_relation) %}

      -- this may no-op if the database does not require column expansion
      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {# Snapshot Column Backfill: Optionally backfill historical rows with current source values #}
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
              model['compiled_code'],
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

      {% set final_sql = snapshot_merge_sql(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
         )
      %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {% set should_revoke = should_revoke(target_relation_exists, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

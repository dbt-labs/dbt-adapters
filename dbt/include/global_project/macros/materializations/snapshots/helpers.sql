{#
    Add new columns to the table if applicable
#}
{% macro create_columns(relation, columns) %}
  {{ adapter.dispatch('create_columns', 'dbt')(relation, columns) }}
{% endmacro %}

{% macro default__create_columns(relation, columns) %}
  {% for column in columns %}
    {% call statement() %}
      alter table {{ relation.render() }} add column "{{ column.name }}" {{ column.data_type }};
    {% endcall %}
  {% endfor %}
{% endmacro %}


{% macro post_snapshot(staging_relation) %}
  {{ adapter.dispatch('post_snapshot', 'dbt')(staging_relation) }}
{% endmacro %}

{% macro default__post_snapshot(staging_relation) %}
    {# no-op #}
{% endmacro %}

{% macro get_true_sql() %}
  {{ adapter.dispatch('get_true_sql', 'dbt')() }}
{% endmacro %}

{% macro default__get_true_sql() %}
    {{ return('TRUE') }}
{% endmacro %}

{% macro snapshot_staging_table(strategy, source_sql, target_relation) -%}
  {{ adapter.dispatch('snapshot_staging_table', 'dbt')(strategy, source_sql, target_relation) }}
{% endmacro %}

{% macro get_snapshot_table_column_names() %}
    {{ return({'dbt_valid_to': 'dbt_valid_to', 'dbt_valid_from': 'dbt_valid_from', 'dbt_scd_id': 'dbt_scd_id', 'dbt_updated_at': 'dbt_updated_at'}) }}
{% endmacro %}

{% macro default__snapshot_staging_table(strategy, source_sql, target_relation) -%}
    {% set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() %}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select *,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation }}
        where {{ columns.dbt_valid_to }} is null

    ),

    insertions_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as {{ columns.dbt_valid_to }},
            {{ strategy.scd_id }} as {{ columns.dbt_scd_id }}

        from snapshot_query
    ),

    updates_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_to }}

        from snapshot_query
    ),

    {%- if strategy.invalidate_hard_deletes %}

    deletes_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.{{ columns.dbt_scd_id }}

        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (

        select
            'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as {{ columns.dbt_valid_from }},
            {{ snapshot_get_time() }} as {{ columns.dbt_updated_at }},
            {{ snapshot_get_time() }} as {{ columns.dbt_valid_to }},
            snapshotted_data.{{ columns.dbt_scd_id }}

        from snapshotted_data
        left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}

{%- endmacro %}


{% macro build_snapshot_table(strategy, sql) -%}
  {{ adapter.dispatch('build_snapshot_table', 'dbt')(strategy, sql) }}
{% endmacro %}

{% macro default__build_snapshot_table(strategy, sql) %}
    {% set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() %}

    select *,
        {{ strategy.scd_id }} as {{ columns.dbt_scd_id }},
        {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
        {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as {{ columns.dbt_valid_to }}
    from (
        {{ sql }}
    ) sbq

{% endmacro %}


{% macro build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set temp_relation = make_temp_relation(target_relation) %}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation') %}
        {{ create_table_as(True, temp_relation, select) }}
    {% endcall %}

    {% do return(temp_relation) %}
{% endmacro %}


{% macro get_updated_at_column_data_type(snapshot_sql) %}
    {% set snapshot_sql_column_schema = get_column_schema_from_query(snapshot_sql) %}
    {% set dbt_updated_at_data_type = null %}
    {% set ns = namespace() -%} {#-- handle for-loop scoping with a namespace --#}
    {% set ns.dbt_updated_at_data_type = null -%}
    {% for column in snapshot_sql_column_schema %}
    {%   if ((column.column == 'dbt_updated_at') or (column.column == 'DBT_UPDATED_AT')) %}
    {%     set ns.dbt_updated_at_data_type = column.dtype %}
    {%   endif %}
    {% endfor %}
    {{ return(ns.dbt_updated_at_data_type or none)  }}
{% endmacro %}


{% macro check_time_data_types(sql) %}
  {% set dbt_updated_at_data_type = get_updated_at_column_data_type(sql) %}
  {% set snapshot_get_time_data_type = get_snapshot_get_time_data_type() %}
  {% if snapshot_get_time_data_type is not none and dbt_updated_at_data_type is not none and snapshot_get_time_data_type != dbt_updated_at_data_type %}
  {%   if exceptions.warn_snapshot_timestamp_data_types %}
  {{     exceptions.warn_snapshot_timestamp_data_types(snapshot_get_time_data_type, dbt_updated_at_data_type) }}
  {%   endif %}
  {% endif %}
{% endmacro %}


{% macro postgres__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}

    update {{ target }}
    set {{ columns.dbt_valid_to }} = DBT_INTERNAL_SOURCE.{{ columns.dbt_valid_to }}
    from {{ source }} as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.{{ columns.dbt_scd_id }}::text = {{ target }}.{{ columns.dbt_scd_id }}::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      {% if config.get("dbt_valid_to_current") %}
        and ({{ target }}.{{ columns.dbt_valid_to }} = {{ config.get('dbt_valid_to_current') }} or {{ target }}.{{ columns.dbt_valid_to }} is null);
      {% else %}
        and {{ target }}.{{ columns.dbt_valid_to }} is null;
      {% endif %}


    insert into {{ target }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
        DBT_INTERNAL_SOURCE.{{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ source }} as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;
{% endmacro %}


{# Postgres-specific ISO8601 timestamp for backfill audit column #}
{% macro postgres__snapshot_backfill_timestamp() %}
    {{ return("to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')") }}
{% endmacro %}


{# Postgres backfill uses UPDATE...FROM which is the default behavior #}
{% macro postgres__backfill_snapshot_columns(relation, columns, source_sql, unique_key, audit_column) %}
    {%- set column_names = columns | map(attribute='name') | list -%}
    
    {% call statement('backfill_snapshot_columns') %}
    UPDATE {{ relation.render() }} AS dbt_backfill_target
    SET 
        {%- for col in columns %}
        {{ adapter.quote(col.name) }} = dbt_backfill_source.{{ adapter.quote(col.name) }}
        {%- if not loop.last or audit_column %},{% endif %}
        {%- endfor %}
        {%- if audit_column %}
        {{ adapter.quote(audit_column) }} = CASE 
            WHEN dbt_backfill_target.{{ adapter.quote(audit_column) }} IS NULL THEN 
                '{' || {{ backfill_audit_json_entries(columns) }} || '}'
            ELSE 
                SUBSTRING(dbt_backfill_target.{{ adapter.quote(audit_column) }}, 1, LENGTH(dbt_backfill_target.{{ adapter.quote(audit_column) }}) - 1) 
                || ', ' || {{ backfill_audit_json_entries(columns) }} || '}'
        END
        {%- endif %}
    FROM ({{ source_sql }}) AS dbt_backfill_source
    WHERE {{ backfill_unique_key_join(unique_key, 'dbt_backfill_target', 'dbt_backfill_source') }}
    {% endcall %}
    
    {{ log("WARNING: Backfilling " ~ columns | length ~ " new column(s) [" ~ column_names | join(', ') ~ "] in snapshot '" ~ relation.identifier ~ "'. Historical rows will be populated with CURRENT source values, not point-in-time historical values.", info=true) }}
{% endmacro %}

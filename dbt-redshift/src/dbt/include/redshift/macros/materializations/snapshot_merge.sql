
{% macro redshift__snapshot_merge_sql(target, source, insert_cols) -%}
    {{ postgres__snapshot_merge_sql(target, source, insert_cols) }}
{% endmacro %}


{# Redshift-specific ISO8601 timestamp for backfill audit column #}
{% macro redshift__snapshot_backfill_timestamp() %}
    {{ return("TO_CHAR(GETDATE(), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')") }}
{% endmacro %}


{# Redshift backfill uses UPDATE...FROM similar to Postgres #}
{% macro redshift__backfill_snapshot_columns(relation, columns, source_sql, unique_key, audit_column) %}
    {%- set column_names = columns | map(attribute='name') | list -%}
    
    {% call statement('backfill_snapshot_columns') %}
    UPDATE {{ relation.render() }}
    SET 
        {%- for col in columns %}
        {{ adapter.quote(col.name) }} = dbt_backfill_source.{{ adapter.quote(col.name) }}
        {%- if not loop.last or audit_column %},{% endif %}
        {%- endfor %}
        {%- if audit_column %}
        {{ adapter.quote(audit_column) }} = CASE 
            WHEN {{ relation.render() }}.{{ adapter.quote(audit_column) }} IS NULL THEN 
                '{' || {{ backfill_audit_json_entries(columns) }} || '}'
            ELSE 
                SUBSTRING({{ relation.render() }}.{{ adapter.quote(audit_column) }}, 1, LEN({{ relation.render() }}.{{ adapter.quote(audit_column) }}) - 1) 
                || ', ' || {{ backfill_audit_json_entries(columns) }} || '}'
        END
        {%- endif %}
    FROM ({{ source_sql }}) AS dbt_backfill_source
    WHERE {{ backfill_unique_key_join(unique_key, relation.render(), 'dbt_backfill_source') }}
    {% endcall %}
    
    {{ log("WARNING: Backfilling " ~ columns | length ~ " new column(s) [" ~ column_names | join(', ') ~ "] in snapshot '" ~ relation.identifier ~ "'. Historical rows will be populated with CURRENT source values, not point-in-time historical values.", info=true) }}
{% endmacro %}

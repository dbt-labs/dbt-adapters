{% materialization snapshot, adapter='snowflake' %}
    {% set original_query_tag = set_query_tag() %}
    {% set relations = materialization_snapshot_default() %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return(relations) }}
{% endmaterialization %}


{# Snowflake-specific ISO8601 timestamp for backfill audit column #}
{% macro snowflake__snapshot_backfill_timestamp() %}
    {{ return("TO_CHAR(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')") }}
{% endmacro %}


{# Snowflake backfill uses UPDATE...FROM which is the default behavior #}
{% macro snowflake__backfill_snapshot_columns(relation, columns, source_sql, unique_key, audit_column) %}
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
                SUBSTR(dbt_backfill_target.{{ adapter.quote(audit_column) }}, 1, LENGTH(dbt_backfill_target.{{ adapter.quote(audit_column) }}) - 1)
                || ', ' || {{ backfill_audit_json_entries(columns) }} || '}'
        END
        {%- endif %}
    FROM ({{ source_sql }}) AS dbt_backfill_source
    WHERE {{ backfill_unique_key_join(unique_key, 'dbt_backfill_target', 'dbt_backfill_source') }}
    {% endcall %}

    {{ log("WARNING: Backfilling " ~ columns | length ~ " new column(s) [" ~ column_names | join(', ') ~ "] in snapshot '" ~ relation.identifier ~ "'. Historical rows will be populated with CURRENT source values, not point-in-time historical values.", info=true) }}
{% endmacro %}

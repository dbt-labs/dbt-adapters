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


{# Snowflake backfill uses UPDATE...FROM syntax #}
{# Note: Snowflake stores unquoted identifiers as uppercase, so we avoid quoting #}
{# column names in joins to prevent case-sensitivity issues #}
{% macro snowflake__backfill_snapshot_columns(relation, columns, source_sql, unique_key, audit_column) %}
    {%- set column_names = columns | map(attribute='name') | list -%}

    {% call statement('backfill_snapshot_columns') %}
    UPDATE {{ relation.render() }} AS dbt_backfill_target
    SET
        {%- for col in columns %}
        {{ col.name }} = dbt_backfill_source.{{ col.name }}
        {%- if not loop.last or audit_column %},{% endif %}
        {%- endfor %}
        {%- if audit_column %}
        {{ audit_column }} = CASE
            WHEN dbt_backfill_target.{{ audit_column }} IS NULL THEN
                '{' || {{ backfill_audit_json_entries(columns) }} || '}'
            ELSE
                SUBSTR(dbt_backfill_target.{{ audit_column }}, 1, LENGTH(dbt_backfill_target.{{ audit_column }}) - 1)
                || ', ' || {{ backfill_audit_json_entries(columns) }} || '}'
        END
        {%- endif %}
    FROM ({{ source_sql }}) AS dbt_backfill_source
    WHERE {{ snowflake__backfill_unique_key_join(unique_key, 'dbt_backfill_target', 'dbt_backfill_source') }}
    {% endcall %}

    {{ log("WARNING: Backfilling " ~ columns | length ~ " new column(s) [" ~ column_names | join(', ') ~ "] in snapshot '" ~ relation.identifier ~ "'. Historical rows will be populated with CURRENT source values, not point-in-time historical values.", info=true) }}
{% endmacro %}


{# Snowflake-specific unique key join - uses unquoted identifiers to avoid case issues #}
{% macro snowflake__backfill_unique_key_join(unique_key, target_alias, source_alias) %}
    {% if unique_key | is_list %}
        {% for key in unique_key %}
            {{ target_alias }}.{{ key }} = {{ source_alias }}.{{ key }}
            {%- if not loop.last %} AND {% endif %}
        {% endfor %}
    {% else %}
        {{ target_alias }}.{{ unique_key }} = {{ source_alias }}.{{ unique_key }}
    {% endif %}
{% endmacro %}

{% macro bigquery__snapshot_hash_arguments(args) -%}
  to_hex(md5(concat({%- for arg in args -%}
    coalesce(cast({{ arg }} as string), ''){% if not loop.last %}, '|',{% endif -%}
  {%- endfor -%}
  )))
{%- endmacro %}

{% macro bigquery__create_columns(relation, columns) %}
  {{ adapter.alter_table_add_columns(relation, columns) }}
{% endmacro %}

{% macro bigquery__post_snapshot(staging_relation) %}
  -- Clean up the snapshot temp table
  {% do drop_relation(staging_relation) %}
{% endmacro %}


{# BigQuery-specific ISO8601 timestamp for backfill audit column #}
{% macro bigquery__snapshot_backfill_timestamp() %}
    {{ return("FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', CURRENT_TIMESTAMP(), 'UTC')") }}
{% endmacro %}


{# BigQuery-specific JSON entries builder #}
{% macro bigquery__backfill_audit_json_entries(columns) %}
    {%- set entries = [] -%}
    {%- for col in columns -%}
        {%- do entries.append("'\"" ~ col.name ~ "\": \"' || " ~ snapshot_backfill_timestamp() ~ " || '\"'") -%}
    {%- endfor -%}
    {{ return("CONCAT(" ~ entries | join(", ', ', ") ~ ")") }}
{% endmacro %}


{# BigQuery backfill using MERGE syntax #}
{% macro bigquery__backfill_snapshot_columns(relation, columns, source_sql, unique_key, audit_column) %}
    {%- set column_names = columns | map(attribute='name') | list -%}

    {% call statement('backfill_snapshot_columns') %}
    MERGE INTO {{ relation.render() }} AS dbt_backfill_target
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
                CONCAT('{', {{ backfill_audit_json_entries(columns) }}, '}')
            ELSE
                CONCAT(
                    SUBSTR(dbt_backfill_target.{{ adapter.quote(audit_column) }}, 1, LENGTH(dbt_backfill_target.{{ adapter.quote(audit_column) }}) - 1),
                    ', ',
                    {{ backfill_audit_json_entries(columns) }},
                    '}'
                )
        END
        {%- endif %}
    {% endcall %}

    {{ log("WARNING: Backfilling " ~ columns | length ~ " new column(s) [" ~ column_names | join(', ') ~ "] in snapshot '" ~ relation.identifier ~ "'. Historical rows will be populated with CURRENT source values, not point-in-time historical values.", info=true) }}
{% endmacro %}

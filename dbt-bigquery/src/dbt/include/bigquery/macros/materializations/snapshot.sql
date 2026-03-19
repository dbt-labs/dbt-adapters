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

{% macro bigquery__snapshot_check_row_changed(check_cols, snapshotted_rel, current_rel, node) %}
    {#-- Detect REPEATED (ARRAY) columns that need TO_JSON_STRING wrapping
         since BigQuery does not support != on ARRAY types. --#}
    {% set repeated_cols = [] %}
    {% for col in adapter.get_columns_in_select_sql(node['compiled_code']) %}
        {% if col.mode == 'REPEATED' %}
            {% do repeated_cols.append(adapter.quote(col.column)) %}
        {% endif %}
    {% endfor %}

    {%- for col in check_cols -%}
        {%- if col in repeated_cols -%}
        TO_JSON_STRING({{ snapshotted_rel }}.{{ col }}) != TO_JSON_STRING({{ current_rel }}.{{ col }})
        {%- else -%}
        {{ snapshotted_rel }}.{{ col }} != {{ current_rel }}.{{ col }}
        {%- endif %}
        or
        (
            (({{ snapshotted_rel }}.{{ col }} is null) and not ({{ current_rel }}.{{ col }} is null))
            or
            ((not {{ snapshotted_rel }}.{{ col }} is null) and ({{ current_rel }}.{{ col }} is null))
        )
        {%- if not loop.last %} or {% endif -%}
    {%- endfor -%}
{% endmacro %}

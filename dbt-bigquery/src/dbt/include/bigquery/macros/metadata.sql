{% macro bigquery__get_relation_last_modified(information_schema, relations) -%}
  {% set unique_schemas = (relations | map(attribute='schema')) | list | unique %}

  {% set table_ids = (relations | map(attribute='identifier')) | list %}
  {% set table_ids_formatted = [] %}
  {% for table_id in table_ids %}
    {% do table_ids_formatted.append("'" + table_id + "'") %}
  {% endfor %}


  {%- call statement('last_modified', fetch_result=True) -%}
  {%- for unique_schema in unique_schemas -%}
        select
            dataset_id AS schema,
            table_id AS identifier,
            TIMESTAMP_MILLIS(last_modified_time) AS last_modified,
            {{ current_timestamp() }} as snapshotted_at
        from {{ unique_schema }}.__TABLES__
        where (table_id in ({{ table_ids_formatted | join(',') }}))

        {{ 'union all\n' if not loop.last else "" }}
  {%- endfor -%}
  {%- endcall -%}

  {{ return(load_result('last_modified')) }}

{% endmacro %}

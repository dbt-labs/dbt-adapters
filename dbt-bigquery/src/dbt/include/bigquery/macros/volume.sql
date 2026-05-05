{% macro bigquery__collect_source_volume(database, schema, identifier) %}
  {# Queries TABLE_STORAGE for total_rows of a single table #}
  {% set location = adapter.get_dataset_location(
      api.Relation.create(database=database, schema=schema, identifier=identifier)
  ) %}
  {% set information_schema = api.Relation.create(
      database=database, schema=schema, identifier=identifier
  ).incorporate(location=location).information_schema("TABLE_STORAGE") %}

  {% call statement('collect_source_volume', fetch_result=True, auto_begin=False) -%}
    select
      table_name as entity_name,
      total_rows,
      {{ current_timestamp() }} as checked_at
    from {{ information_schema }}
    where upper(table_schema) = upper('{{ schema }}')
      and upper(table_name) = upper('{{ identifier }}')
      and total_rows is not null
  {%- endcall %}

  {{ return(load_result('collect_source_volume')) }}
{% endmacro %}

{% macro bigquery__collect_source_volume_wildcard(database, schema, table_pattern) %}
  {# Queries TABLE_STORAGE for tables matching a pattern and their total_rows #}
  {% set location = adapter.get_dataset_location(
      api.Relation.create(database=database, schema=schema, identifier='__placeholder__')
  ) %}
  {% set information_schema = api.Relation.create(
      database=database, schema=schema, identifier='__placeholder__'
  ).incorporate(location=location).information_schema("TABLE_STORAGE") %}

  {% call statement('collect_source_volume_wildcard', fetch_result=True, auto_begin=False) -%}
    select
      table_name as entity_name,
      total_rows,
      {{ current_timestamp() }} as checked_at
    from {{ information_schema }}
    where upper(table_schema) = upper('{{ schema }}')
      and REGEXP_CONTAINS(table_name, r'{{ table_pattern }}')
      and total_rows is not null
    order by table_name
  {%- endcall %}

  {{ return(load_result('collect_source_volume_wildcard')) }}
{% endmacro %}

{% macro bigquery__collect_source_volume_partitions(database, schema, identifier, partition_field, partition_range) %}
  {# Queries INFORMATION_SCHEMA.PARTITIONS for the N most recent partitions.
     partition_field is accepted for parity with other adapters but is unused here
     because BigQuery tables have a single partition scheme. #}
  {% set location = adapter.get_dataset_location(
      api.Relation.create(database=database, schema=schema, identifier=identifier)
  ) %}
  {% set information_schema = api.Relation.create(
      database=database, schema=schema, identifier=identifier
  ).incorporate(location=location).information_schema("PARTITIONS") %}

  {% call statement('collect_source_volume_partitions', fetch_result=True, auto_begin=False) -%}
    select
      partition_id as entity_name,
      total_rows,
      {{ current_timestamp() }} as checked_at
    from {{ information_schema }}
    where upper(table_schema) = upper('{{ schema }}')
      and upper(table_name) = upper('{{ identifier }}')
      and partition_id != '__NULL__'
      and total_rows is not null
    order by partition_id desc
    limit {{ partition_range }}
  {%- endcall %}

  {{ return(load_result('collect_source_volume_partitions')) }}
{% endmacro %}

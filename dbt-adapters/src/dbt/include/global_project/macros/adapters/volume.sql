{% macro collect_source_volume(database, schema, identifier) %}
  {{ return(adapter.dispatch('collect_source_volume', 'dbt')(database, schema, identifier)) }}
{% endmacro %}

{% macro default__collect_source_volume(database, schema, identifier) %}
  {{ exceptions.raise_not_implemented('collect_source_volume macro not implemented for adapter ' ~ adapter.type()) }}
{% endmacro %}

{% macro collect_source_volume_wildcard(database, schema, table_pattern) %}
  {{ return(adapter.dispatch('collect_source_volume_wildcard', 'dbt')(database, schema, table_pattern)) }}
{% endmacro %}

{% macro default__collect_source_volume_wildcard(database, schema, table_pattern) %}
  {{ exceptions.raise_not_implemented('collect_source_volume_wildcard macro not implemented for adapter ' ~ adapter.type()) }}
{% endmacro %}

{% macro collect_source_volume_partitions(database, schema, identifier, partition_field, partition_range) %}
  {{ return(adapter.dispatch('collect_source_volume_partitions', 'dbt')(database, schema, identifier, partition_field, partition_range)) }}
{% endmacro %}

{% macro default__collect_source_volume_partitions(database, schema, identifier, partition_field, partition_range) %}
  {{ exceptions.raise_not_implemented('collect_source_volume_partitions macro not implemented for adapter ' ~ adapter.type()) }}
{% endmacro %}

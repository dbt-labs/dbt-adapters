{% macro bigquery__create_schema(relation) -%}
  {%- set dataset_replicas = config.get('dataset_replicas') -%}
  {%- set primary_replica = config.get('primary_replica') -%}

  {# Normalize dataset_replicas to a list of strings #}
  {%- if dataset_replicas is string -%}
    {# Allow comma-separated strings #}
    {%- set dataset_replicas = dataset_replicas.split(',') | map('trim') | list -%}
  {%- endif -%}
  {%- if dataset_replicas is not none and dataset_replicas is not sequence -%}
    {{ log("Invalid dataset_replicas config; expected list or comma-separated string. Skipping replication.", info=True) }}
    {%- set dataset_replicas = none -%}
  {%- endif -%}

  {%- if dataset_replicas -%}
    {{ log("Configuring dataset " ~ relation.schema ~ " with replicas: " ~ dataset_replicas | join(', '), info=True) }}
    {%- if primary_replica -%}
      {{ log("  Primary replica: " ~ primary_replica, info=True) }}
    {%- endif -%}
  {%- endif -%}

  {% do adapter.create_dataset_with_replication(relation, dataset_replicas, primary_replica) %}
{% endmacro %}

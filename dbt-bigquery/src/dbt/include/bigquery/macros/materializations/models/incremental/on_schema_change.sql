{# this custom implementation is meant to handle schema changes in BigQuery inscluding STRUCT column related changes #}
{% macro bigquery__check_for_schema_changes(source_relation, target_relation) %}

  {% set schema_changed = False %}

  {%- set source_columns = adapter.get_columns_in_relation(source_relation) -%}
  {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set source_not_in_target = diff_columns(source_columns, target_columns) -%}
  {%- set target_not_in_source = diff_columns(target_columns, source_columns) -%}

  {% set new_target_types = diff_column_data_types(source_columns, target_columns) %}

  {% if source_not_in_target != [] %}
    {% set schema_changed = True %}
  {% elif target_not_in_source != [] or new_target_types != [] %}
    {% set schema_changed = True %}
  {% elif new_target_types != [] %}
    {% set schema_changed = True %}
  {% endif %}

  {% set changes_dict = {
    'schema_changed': schema_changed,
    'source_not_in_target': source_not_in_target,
    'target_not_in_source': target_not_in_source,
    'source_columns': source_columns,
    'target_columns': target_columns,
    'new_target_types': new_target_types
  } %}

  {% set on_schema_change = config.get('on_schema_change') %}
  {% set changes_dict = adapter.sync_struct_columns(on_schema_change, source_relation, target_relation, changes_dict) %}
  {% set schema_changed = changes_dict['schema_changed'] %}
  {% set source_not_in_target = changes_dict['source_not_in_target'] %}
  {% set target_not_in_source = changes_dict['target_not_in_source'] %}
  {% set new_target_types = changes_dict['new_target_types'] %}

  {% set msg %}
    In {{ target_relation }}:
        Schema changed: {{ schema_changed }}
        Source columns not in target: {{ source_not_in_target }}
        Target columns not in source: {{ target_not_in_source }}
        New column types: {{ new_target_types }}
  {% endset %}

  {% do log(msg) %}

  {{ return(changes_dict) }}

{% endmacro %}

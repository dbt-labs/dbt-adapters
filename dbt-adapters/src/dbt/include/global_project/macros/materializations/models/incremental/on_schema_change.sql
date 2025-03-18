{% macro incremental_validate_on_schema_change(on_schema_change, default='ignore') %}

   {% if on_schema_change not in ['sync_all_columns', 'append_new_columns', 'fail', 'ignore', 'full_refresh'] %}

     {% set log_message = 'Invalid value for on_schema_change (%s) specified. Setting default value of %s.' % (on_schema_change, default) %}
     {% do log(log_message) %}

     {{ return(default) }}

   {% else %}

     {{ return(on_schema_change) }}

   {% endif %}

{% endmacro %}


{% macro check_for_schema_changes(source_relation, target_relation) %}

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


{% macro sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}

  {%- set add_to_target_arr = schema_changes_dict['source_not_in_target'] -%}

  {%- if on_schema_change == 'append_new_columns'-%}
     {%- if add_to_target_arr | length > 0 -%}
       {%- do alter_relation_add_remove_columns(target_relation, add_to_target_arr, none) -%}
     {%- endif -%}

  {% elif on_schema_change == 'sync_all_columns' %}
     {%- set remove_from_target_arr = schema_changes_dict['target_not_in_source'] -%}
     {%- set new_target_types = schema_changes_dict['new_target_types'] -%}

     {% if add_to_target_arr | length > 0 or remove_from_target_arr | length > 0 %}
       {%- do alter_relation_add_remove_columns(target_relation, add_to_target_arr, remove_from_target_arr) -%}
     {% endif %}

     {% if new_target_types != [] %}
       {% for ntt in new_target_types %}
         {% set column_name = ntt['column_name'] %}
         {% set new_type = ntt['new_type'] %}
         {% do alter_column_type(target_relation, column_name, new_type) %}
       {% endfor %}
     {% endif %}

  {% endif %}

  {% set schema_change_message %}
    In {{ target_relation }}:
        Schema change approach: {{ on_schema_change }}
        Columns added: {{ add_to_target_arr }}
        Columns removed: {{ remove_from_target_arr }}
        Data types changed: {{ new_target_types }}
  {% endset %}

  {% do log(schema_change_message) %}

{% endmacro %}


{% macro process_schema_changes(on_schema_change, source_relation, target_relation) %}

    {% if on_schema_change in ['ignore', 'full_refresh'] %}

     {{ return({}) }}

    {% else %}

      {% set schema_changes_dict = check_for_schema_changes(source_relation, target_relation) %}

      {% if schema_changes_dict['schema_changed'] %}

        {% if on_schema_change == 'fail' %}

          {% set fail_msg %}
              The source and target schemas on this incremental model are out of sync!
              They can be reconciled in several ways:
                - set the `on_schema_change` config to either append_new_columns, sync_all_columns or full_refresh, depending on your situation.
                - update the schema manually and re-run the process.

              Additional troubleshooting context:
                 Source columns not in target: {{ schema_changes_dict['source_not_in_target'] }}
                 Target columns not in source: {{ schema_changes_dict['target_not_in_source'] }}
                 New column types: {{ schema_changes_dict['new_target_types'] }}
          {% endset %}

          {% do exceptions.raise_compiler_error(fail_msg) %}

        {# -- unless we ignore, run the sync operation per the config #}
        {% else %}

          {% do sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}

        {% endif %}

      {% endif %}

      {{ return(schema_changes_dict['source_columns']) }}

    {% endif %}

{% endmacro %}


{% macro on_schema_change_full_refresh(on_schema_change, existing_relation) %}
  {% if on_schema_change == 'full_refresh' and existing_relation is not none and existing_relation.is_view == false %}
      {# we can only check column names and not types because get_columns_in_query only return names #}
      {%- set existing_columns_with_types = adapter.get_columns_in_relation(existing_relation) -%}
      {%- set new_columns = get_columns_in_query(sql) -%}

      {% set ns = namespace(schema_changed=False) %}

      {% set existing_columns = [] %}
      {% for existing_column in existing_columns_with_types %}
        {% if existing_column.name not in new_columns %}
          {% set ns.schema_changed = True %}
        {% endif %}
        {% set existing_columns = existing_columns.append(existing_column.name) %}
      {% endfor %}

      {% for new_column in new_columns %}
        {% if new_column not in existing_columns %}
          {% set ns.schema_changed = True %}
        {% endif %}
      {% endfor %}

      {% if ns.schema_changed %}
        {{ return(True) }}
      {% endif %}
  {% endif %}
  {{ return(False) }}
{% endmacro %}

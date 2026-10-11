{% macro incremental_validate_on_schema_change(on_schema_change, default='ignore') %}
  {% set schema_change_plan = adapter.plan_incremental_schema_change(on_schema_change, default) %}
  {% if schema_change_plan.was_coerced %}
    {% do log(schema_change_plan.provenance[-1].detail) %}
  {% endif %}
  {{ return(schema_change_plan.strategy.value) }}
{% endmacro %}


{% macro check_for_schema_changes(source_relation, target_relation) %}
  {{ return(adapter.dispatch('check_for_schema_changes', 'dbt')(source_relation, target_relation)) }}
{% endmacro %}


{% macro default__check_for_schema_changes(source_relation, target_relation) %}

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
  {{ return(adapter.dispatch('sync_column_schemas', 'dbt')(on_schema_change, target_relation, schema_changes_dict)) }}
{% endmacro %}


{% macro default__sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}

  {%- set add_to_target_arr = schema_changes_dict['source_not_in_target'] -%}
  {%- set remove_from_target_arr = schema_changes_dict['target_not_in_source'] -%}
  {%- set new_target_types = schema_changes_dict['new_target_types'] -%}

  {%- if on_schema_change == 'append_new_columns'-%}
     {%- if add_to_target_arr | length > 0 -%}
       {%- do alter_relation_add_remove_columns(target_relation, add_to_target_arr, none) -%}
     {%- endif -%}

  {% elif on_schema_change == 'sync_all_columns' %}

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
  {{ return(adapter.dispatch('process_schema_changes', 'dbt')(on_schema_change, source_relation, target_relation)) }}
{% endmacro %}


{% macro default__process_schema_changes(on_schema_change, source_relation, target_relation) %}

    {% if on_schema_change == 'ignore' %}

     {{ return({}) }}

    {% else %}

      {% set schema_changes_dict = check_for_schema_changes(source_relation, target_relation) %}

      {#-
        An empty source column list is an introspection failure, not a real schema -- this run just
        built source_relation. sync_all_columns would drop every target column and fail would report
        a bogus diff, so both refuse. append_new_columns never drops, and the caller substitutes the
        target's columns for our empty return, so it warns instead of breaking a working run.
      -#}
      {%- if schema_changes_dict['source_columns'] | length == 0 -%}
        {%- set empty_source_columns_msg -%}
          Could not read any columns for {{ source_relation }} while checking for schema changes on
          incremental model {{ target_relation }} (on_schema_change='{{ on_schema_change }}').

          This is a metadata failure rather than an empty schema: {{ source_relation }} was just
          built by this run, so it does have columns.

          Common causes:
            - the warehouse does not expose this temporary relation to the catalog views the
              adapter reads column metadata from
            - the connection's database differs from the one the temporary relation was created in

          {% if on_schema_change == 'append_new_columns' -%}
          Continuing, because on_schema_change='append_new_columns' never drops columns and dbt
          will insert using {{ target_relation }}'s existing columns. No new column can be
          detected this run, so if the model added one it stays missing until introspection works
          or you run with --full-refresh.
          {%- else -%}
          Refusing to continue: dbt cannot determine which columns changed, so it cannot safely
          apply on_schema_change='{{ on_schema_change }}'. With 'sync_all_columns' this would
          silently drop every column in {{ target_relation }}.

          Workarounds:
            - set on_schema_change='ignore' to skip this check
            - run the model with --full-refresh to rebuild it
          {%- endif %}
        {%- endset -%}
        {%- if on_schema_change == 'append_new_columns' -%}
          {%- do exceptions.warn(empty_source_columns_msg) -%}
        {%- else -%}
          {%- do exceptions.raise_compiler_error(empty_source_columns_msg) -%}
        {%- endif -%}
      {%- endif -%}

      {% if schema_changes_dict['schema_changed'] %}

        {% if on_schema_change == 'fail' %}

          {% set fail_msg %}
              The source and target schemas on this incremental model are out of sync!
              They can be reconciled in several ways:
                - set the `on_schema_change` config to either append_new_columns or sync_all_columns, depending on your situation.
                - Re-run the incremental model with `full_refresh: True` to update the target schema.
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

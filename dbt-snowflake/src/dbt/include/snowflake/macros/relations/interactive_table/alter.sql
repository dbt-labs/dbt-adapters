{% macro snowflake__get_alter_interactive_table_as_sql(existing_relation, configuration_changes, target_relation, sql) -%}
    {{ log('Applying ALTER to: ' ~ existing_relation) }}
    {{ snowflake__alter_interactive_table_sql(existing_relation, configuration_changes, target_relation) }}
{%- endmacro %}


{% macro snowflake__alter_interactive_table_sql(existing_relation, configuration_changes, target_relation) %}
    {{- snowflake__get_target_lag_warehouse_alter_sql(
        'interactive',
        existing_relation,
        configuration_changes.target_lag,
        configuration_changes.refresh_warehouse,
        configuration_changes.snowflake_initialization_warehouse
    ) -}}
{% endmacro %}

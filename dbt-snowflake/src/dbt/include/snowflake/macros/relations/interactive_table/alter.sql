{% macro snowflake__get_alter_interactive_table_as_sql(existing_relation, configuration_changes, target_relation, sql) -%}
    {{ log('Applying ALTER to: ' ~ existing_relation) }}
    {{ snowflake__alter_interactive_table_sql(existing_relation, configuration_changes, target_relation) }}
{%- endmacro %}


{% macro snowflake__alter_interactive_table_sql(existing_relation, configuration_changes, target_relation) %}
{#-
    Only reached when `configuration_changes.requires_full_refresh` is False --
    materializations/interactive_table.sql branches on that flag before ever calling this
    macro, so a `cluster_by` change or a dynamic<->static `target_lag` transition never
    reaches here; those route through `get_replace_sql` instead.

    target_lag/warehouse/initialization_warehouse share identical SET/UNSET semantics with
    dynamic_table's changeset -- factored into `relations/target_lag_warehouse_alter.sql` so the
    two can't drift independently (confirmed live against a real Snowflake account with the
    interactive-table feature enabled, 2026-08-25, before the extraction).

    Values are read from `configuration_changes.<field>.context` -- the new/desired value
    the changeset already carries for a value-to-value `alter` -- not re-derived via
    `target_relation.from_config(config.model)`.
-#}
    {{- snowflake__get_target_lag_warehouse_alter_sql(
        'interactive',
        existing_relation,
        configuration_changes.target_lag,
        configuration_changes.refresh_warehouse,
        configuration_changes.snowflake_initialization_warehouse
    ) -}}
{% endmacro %}

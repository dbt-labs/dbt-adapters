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

    Mirrors fs's (v2/Fusion) `snowflake__get_alter_interactive_table_as_sql` /
    `snowflake__alter_interactive_table_sql` statement shape exactly -- confirmed live
    against ktb38830, 2026-08-25 -- combining target_lag/warehouse/initialization_warehouse
    into one `alter interactive table t set ...` statement (space-separated assignments,
    not semicolon-chained), plus a separate `unset initialization_warehouse` statement when
    that field is being cleared rather than changed to a new value.

    Values are read from `configuration_changes.<field>.context` -- the new/desired value
    the changeset already carries for a value-to-value `alter` -- not re-derived via
    `target_relation.from_config(config.model)`. This matches both the real fs source and
    this codebase's existing `dynamic_table/alter.sql`.
-#}
    {%- set target_lag = configuration_changes.target_lag -%}
    {%- set refresh_warehouse = configuration_changes.refresh_warehouse -%}
    {%- set init_warehouse = configuration_changes.snowflake_initialization_warehouse -%}

    {#- `.context` distinguishes a new value from a cleared one for
        snowflake_initialization_warehouse specifically -- it is the only one of the three
        that can be cleared while the table stays dynamic. `target_lag`/`refresh_warehouse`
        can't be cleared without a dynamic<->static transition, which `requires_full_refresh`
        already routes to `get_replace_sql` above, so this macro never sees a clear for
        either of them. -#}
    {%- set has_set_changes = target_lag or refresh_warehouse or (init_warehouse and init_warehouse.context) -%}

    {% if has_set_changes -%}
    alter interactive table {{ existing_relation }} set
        {% if target_lag and target_lag.context %}target_lag = '{{ target_lag.context }}'{% endif %}
        {% if refresh_warehouse %}warehouse = {{ refresh_warehouse.context }}{% endif %}
        {% if init_warehouse and init_warehouse.context %}initialization_warehouse = {{ init_warehouse.context }}{% endif %}
    {%- endif %}

    {%- if init_warehouse and not init_warehouse.context %}
    {% if has_set_changes %};{% endif %}
    alter interactive table {{ existing_relation }} unset initialization_warehouse
    {%- endif %}
{% endmacro %}

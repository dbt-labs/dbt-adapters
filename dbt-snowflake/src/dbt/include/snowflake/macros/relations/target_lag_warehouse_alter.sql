{% macro snowflake__target_lag_warehouse_alter_active(target_lag, warehouse, init_warehouse, scheduler=none) %}
{#-
    Whether `target_lag`/`warehouse`/`init_warehouse`/`scheduler` carries a change that
    `snowflake__get_target_lag_warehouse_alter_sql` (sibling macro, this file) will turn into a
    SET or UNSET statement -- unlike that macro's own internal `has_set_changes`, this checks
    `init_warehouse` on presence alone (not `.context`), since a caller deciding whether ITS OWN
    later clause needs a leading semicolon must also count the standalone UNSET statement that
    fires when `init_warehouse` is present but cleared.

    Shared by dynamic_table/alter.sql and interactive_table/alter.sql: both need this to decide
    whether a later ALTER clause of their own requires a leading semicolon. Recomputing the
    condition independently in each caller is how it could drift from what the SQL-building macro
    below actually emits.
-#}
    {% do return(target_lag or warehouse or init_warehouse or scheduler) %}
{% endmacro %}


{% macro snowflake__get_target_lag_warehouse_alter_sql(table_kind, existing_relation, target_lag, warehouse, init_warehouse, scheduler=none) -%}
{#-
    Produce the `alter <table_kind> table ... set target_lag / warehouse /
    initialization_warehouse [/ scheduler]` statement, and the separate
    `... unset initialization_warehouse` statement when it's being cleared.

    Shared by dynamic_table/alter.sql and interactive_table/alter.sql: both configs expose
    target_lag/warehouse/initialization_warehouse with identical SET/UNSET semantics, differing
    only in the noun between `alter` and `table` in the DDL (`dynamic` or `interactive`, passed as
    `table_kind`) and in the underlying changeset field name for the warehouse component
    (`snowflake_warehouse` on a dynamic table, `refresh_warehouse` on an interactive table) --
    resolved by each caller before this macro is reached, so this macro only ever sees the
    already-extracted changeset entry, never the field name.

    `scheduler` is dynamic-table-only and is taken as its own argument rather than read off a
    changeset here, because an interactive table's changeset never carries that component at all.
    `interactive_table/alter.sql` doesn't pass it, so it defaults to `none` and drops out of
    every condition below.

    Args:
    - table_kind: str - 'dynamic' or 'interactive'
    - existing_relation: SnowflakeRelation - the relation being altered
    - target_lag: optional changeset entry for the target_lag component
    - warehouse: optional changeset entry for the refresh/compute warehouse component
    - init_warehouse: optional changeset entry for the initialization_warehouse component
    - scheduler: optional changeset entry for the dynamic-table-only scheduler component
    Returns:
        The SET statement, the UNSET statement, or both (`;`-separated), or an empty string when
        none of the components changed.
-#}
    {%- if target_lag -%}{{- log('Applying UPDATE TARGET_LAG to: ' ~ existing_relation) -}}{%- endif -%}
    {%- if warehouse -%}{{- log('Applying UPDATE WAREHOUSE to: ' ~ existing_relation) -}}{%- endif -%}
    {%- if init_warehouse and init_warehouse.context -%}{{- log('Applying UPDATE INITIALIZATION_WAREHOUSE to: ' ~ existing_relation) -}}{%- endif -%}
    {%- if scheduler -%}{{- log('Applying UPDATE SCHEDULER to: ' ~ existing_relation) -}}{%- endif -%}

    {%- set has_set_changes = target_lag or warehouse or (init_warehouse and init_warehouse.context) or scheduler -%}

    {% if has_set_changes -%}
    alter {{ table_kind }} table {{ existing_relation }} set
        {% if target_lag and target_lag.context %}target_lag = '{{ target_lag.context }}'{% endif %}
        {% if warehouse and warehouse.context %}warehouse = {{ warehouse.context }}{% endif %}
        {% if init_warehouse and init_warehouse.context %}initialization_warehouse = {{ init_warehouse.context }}{% endif %}
        {% if scheduler %}scheduler = '{{ scheduler.context }}'{% endif %}
    {%- endif %}

    {%- if init_warehouse and not init_warehouse.context %}
    {% if has_set_changes %};{% endif %}
    alter {{ table_kind }} table {{ existing_relation }} unset initialization_warehouse
    {%- endif %}
{% endmacro %}

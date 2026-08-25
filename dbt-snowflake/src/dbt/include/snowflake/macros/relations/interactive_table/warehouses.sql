{% macro snowflake__sync_interactive_warehouses(relation) %}
    {#-
        Reconcile which interactive warehouses have `relation` attached against the
        `snowflake_interactive_warehouses` config.

        This association is never part of the interactive table config changeset: it's
        readable only from the warehouse side (`SHOW WAREHOUSES`, see
        `describe_interactive_table_warehouses` in impl.py), not from `SHOW INTERACTIVE
        TABLES`, so it can never be diffed there. It's diffed and reconciled here instead,
        on every run, independent of whether the table itself changed.

        Warehouse identifiers are compared case-insensitively: Snowflake folds unquoted
        identifiers to upper case and echoes `SHOW WAREHOUSES`'s `tables` column back that
        way regardless of how the identifier was originally cased, so a desired `my_wh`
        must match a currently attached `MY_WH` without producing a spurious
        detach-then-reattach.
    -#}
    {%- set desired = config.get('snowflake_interactive_warehouses') -%}
    {%- set desired = ([desired] if desired is string else (desired or [])) -%}
    {%- set current = adapter.describe_interactive_table_warehouses(relation) -%}

    {%- set desired_upper = desired | map('upper') | list -%}
    {%- set current_upper = current | map('upper') | list -%}

    {%- for warehouse in desired if warehouse | upper not in current_upper -%}
        {%- call statement('attach_interactive_warehouse_' ~ loop.index) -%}
            alter warehouse {{ warehouse }} add tables ({{ relation }})
        {%- endcall -%}
    {%- endfor -%}

    {%- for warehouse in current if warehouse | upper not in desired_upper -%}
        {%- call statement('detach_interactive_warehouse_' ~ loop.index) -%}
            alter warehouse {{ warehouse }} drop tables ({{ relation }})
        {%- endcall -%}
    {%- endfor -%}
{% endmacro %}

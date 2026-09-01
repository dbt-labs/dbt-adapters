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
        way regardless of the original casing, so a desired `my_wh` must match a currently
        attached `MY_WH` without producing a spurious detach-then-reattach.

        Attach is unconditional: `describe_interactive_table_warehouses` matches FQNs
        lossily and can false-positive, so gating attach on the diff risks a table never
        being attached; a redundant `ADD TABLES` is a silent no-op. Detach stays
        conditional -- a false-positive detach is harmless.
    -#}
    {%- set desired = config.get('snowflake_interactive_warehouses') -%}
    {%- set desired = ([desired] if desired is string else (desired or [])) -%}
    {%- set current = adapter.describe_interactive_table_warehouses(relation) -%}

    {%- set desired_upper = desired | map('trim') | map('upper') | list -%}

    {%- for warehouse in desired -%}
        {%- call statement('attach_interactive_warehouse_' ~ loop.index) -%}
            alter warehouse {{ warehouse }} add tables ({{ relation }})
        {%- endcall -%}
    {%- endfor -%}

    {%- for warehouse in current if warehouse | trim | upper not in desired_upper -%}
        {%- call statement('detach_interactive_warehouse_' ~ loop.index) -%}
            alter warehouse {{ warehouse }} drop tables ({{ relation }})
        {%- endcall -%}
    {%- endfor -%}
{% endmacro %}

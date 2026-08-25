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
        detach-then-reattach. Comparisons are also trimmed, since a stray leading or
        trailing space in the config wouldn't match a clean identifier from `current`.

        Attach is unconditional: every desired warehouse gets an `ADD TABLES` on every
        run, even ones already attached. `describe_interactive_table_warehouses` does a
        lossy comparison (comma-split, unescaped, case-folded FQN matching against `SHOW
        WAREHOUSES`'s `tables` column) that can produce false positives, e.g. two
        relations differing only by quoted-identifier case can collapse to the same
        uppercased FQN. If attach were conditioned on that diff, a false positive would
        mean a table silently never gets attached. Unconditional attach makes that
        harmless instead: the redundant `ADD TABLES` on an already-attached table
        succeeds silently as a no-op. Detach stays conditional on the diff, since a
        false-positive detach is itself idempotent/harmless (dropping a table that's
        already not attached) and a false-negative just delays cleanup by one run.
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

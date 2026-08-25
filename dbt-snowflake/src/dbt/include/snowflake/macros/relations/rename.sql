{% macro snowflake__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}


{#-
    NOTE: unlike create.sql/drop.sql/replace.sql, there is no pre-existing `is_dynamic_table`
    branch in this file to mirror for `is_interactive_table` -- this file only overrode
    `snowflake__rename_relation` (a separate, legacy macro used by backup/swap flows) before
    this change. The generic `get_rename_sql` / `default__get_rename_sql` dispatcher
    (dbt-adapters' global_project/macros/relations/rename.sql) only branches on
    `relation.is_view` / `is_table` / `is_materialized_view`; dynamic tables only reach
    `dynamic_table/rename.sql` because `SnowflakeRelation.is_materialized_view` is defined to
    return True for `DynamicTable` (relation.py) -- a property alias into that generic bucket,
    not a literal `is_dynamic_table` check anywhere. `is_materialized_view` has several OTHER
    callers in dbt-adapters' own base framework (create.sql, drop.sql, replace.sql, rename.sql,
    materialized_view.sql) -- aliasing `InteractiveTable` into it would widen a property with a
    real blast radius well beyond this file, not a locally-scoped one. Overriding
    `get_rename_sql` directly here, with an explicit `is_interactive_table` branch, reaches
    `get_rename_interactive_table_sql` without
    touching the existing view/table/dynamic_table dispatch path at all.
-#}
{% macro snowflake__get_rename_sql(relation, new_name) -%}

    {% if relation.is_interactive_table %}
        {{ snowflake__get_rename_interactive_table_sql(relation, new_name) }}

    {% else %}
        {{ default__get_rename_sql(relation, new_name) }}

    {% endif %}

{%- endmacro %}

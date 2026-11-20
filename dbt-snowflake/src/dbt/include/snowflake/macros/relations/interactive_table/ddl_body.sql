{% macro snowflake__interactive_table_ddl_body_sql(interactive_table, relation, sql, ddl_prefix) -%}
{#-
    Shared DDL body for CREATE/CREATE OR REPLACE INTERACTIVE TABLE -- see
    https://docs.snowflake.com/en/sql-reference/sql/create-interactive-table
    No COPY GRANTS, no iceberg/table_format variant -- neither appears in the
    documented CREATE INTERACTIVE TABLE syntax.
-#}
    {{ ddl_prefix }} {{ relation }}
        cluster by ({{ interactive_table.cluster_by }})
        {% if interactive_table.is_dynamic %}
        target_lag = '{{ interactive_table.target_lag }}'
        {{ optional('warehouse', interactive_table.warehouse_parameter, equals_char='= ') }}
        {{ optional('initialization_warehouse', interactive_table.snowflake_initialization_warehouse, equals_char='= ') }}
        {% endif %}
        as (
            {{ sql }}
        )
{%- endmacro %}

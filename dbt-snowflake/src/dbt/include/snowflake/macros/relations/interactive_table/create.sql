{% macro snowflake__get_create_interactive_table_as_sql(relation, sql) -%}

    {%- set interactive_table = relation.from_config(config.model) -%}
    {{ snowflake__create_interactive_table_sql(interactive_table, relation, sql) }}

{%- endmacro %}


{% macro snowflake__create_interactive_table_sql(interactive_table, relation, sql) -%}
{#-
    https://docs.snowflake.com/en/sql-reference/sql/create-interactive-table
    No COPY GRANTS, no iceberg/table_format variant -- neither appears in the
    documented CREATE INTERACTIVE TABLE syntax.
-#}

    create interactive table {{ relation }}
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

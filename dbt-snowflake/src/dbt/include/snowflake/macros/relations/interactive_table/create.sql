{% macro snowflake__get_create_interactive_table_as_sql(relation, sql) -%}

    {%- set interactive_table = relation.from_config(config.model) -%}
    {{ snowflake__create_interactive_table_sql(interactive_table, relation, sql) }}

{%- endmacro %}


{% macro snowflake__create_interactive_table_sql(interactive_table, relation, sql) -%}
{#-
    Produce DDL that creates an interactive table

    Implements CREATE INTERACTIVE TABLE:
    https://docs.snowflake.com/en/sql-reference/sql/create-interactive-table

    Args:
    - interactive_table: SnowflakeInteractiveTableConfig
    - relation: Union[SnowflakeRelation, str]
        - SnowflakeRelation - required for relation.render()
        - str - is already the rendered relation name
    - sql: str - the code defining the model
    Returns:
        A valid DDL statement which will result in a new interactive table.
-#}

    create interactive table {{ relation }}
        cluster by ({{ interactive_table.cluster_by }})
        {% if interactive_table.target_lag is not none %}target_lag = '{{ interactive_table.target_lag }}'{% endif %}
        {{ optional('warehouse', interactive_table.snowflake_warehouse, equals_char='= ') }}
        as (
            {{ sql }}
        )

{%- endmacro %}

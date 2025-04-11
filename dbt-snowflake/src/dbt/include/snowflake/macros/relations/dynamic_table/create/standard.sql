{% macro snowflake__create_dynamic_table_standard_sql(dynamic_table, relation, sql) -%}
{#-
    Produce DDL that creates a standard dynamic table

    Implements CREATE DYNAMIC TABLE:
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#syntax

    Args:
    - dynamic_table: SnowflakeDynamicTableConfig - contains all of the configuration for the dynamic table
    - relation: Union[SnowflakeRelation, str]
        - SnowflakeRelation - required for relation.render()
        - str - is already the rendered relation name
    - sql: str - the code defining the model
    Returns:
        A valid DDL statement which will result in a new dynamic standard table.
-#}

create dynamic table {{ relation }}
    target_lag = '{{ dynamic_table.target_lag }}'
    warehouse = {{ dynamic_table.snowflake_warehouse }}
    {{ optional('refresh_mode', dynamic_table.refresh_mode) }}
    {{ optional('initialize', dynamic_table.initialize) }}
    as (
        {{ sql }}
    )

{%- endmacro %}

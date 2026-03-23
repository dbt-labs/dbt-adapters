{% macro snowflake__get_replace_interactive_table_sql(relation, sql) -%}
{#-
    Produce DDL that replaces an interactive table with a new interactive table

    Args:
    - relation: Union[SnowflakeRelation, str]
        - SnowflakeRelation - required for relation.render()
        - str - is already the rendered relation name
    - sql: str - the code defining the model
    Globals:
    - config: NodeConfig - contains the attribution required to produce a SnowflakeInteractiveTableConfig
    Returns:
        A valid DDL statement which will result in a replaced interactive table.
-#}

    {%- set interactive_table = relation.from_config(config.model) -%}

    create or replace interactive table {{ relation }}
        cluster by ({{ interactive_table.cluster_by }})
        {% if interactive_table.target_lag is not none %}target_lag = '{{ interactive_table.target_lag }}'{% endif %}
        {{ optional('warehouse', interactive_table.snowflake_warehouse, equals_char='= ') }}
        as (
            {{ sql }}
        )

{%- endmacro %}

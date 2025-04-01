{% macro snowflake__create_dynamic_table_iceberg_managed_sql(dynamic_table, relation, sql) -%}
{#-
    Produce DDL that creates a dynamic iceberg table

    Implements CREATE DYNAMIC ICEBERG TABLE (Snowflake as the Iceberg catalog):
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#create-dynamic-iceberg-table

    Args:
    - dynamic_table: SnowflakeDynamicTableConfig - contains all of the configuration for the dynamic table
    - relation: Union[SnowflakeRelation, str]
        - SnowflakeRelation - required for relation.render()
        - str - is already the rendered relation name
    - sql: str - the code defining the model
    Returns:
        A valid DDL statement which will result in a new dynamic iceberg table.
-#}

{%- set catalog = adapter.build_catalog_relation(config.model) -%}

create dynamic iceberg table {{ relation }}
    target_lag = '{{ dynamic_table.target_lag }}'
    warehouse = {{ dynamic_table.snowflake_warehouse }}
    {{ optional('external_volume', catalog.external_volume, "'") }}
    catalog = 'SNOWFLAKE'
    base_location = '{{ catalog.base_location }}'
    {{ optional('refresh_mode', dynamic_table.refresh_mode) }}
    {{ optional('initialize', dynamic_table.initialize) }}
    as (
        {{ sql }}
    )

{%- endmacro %}

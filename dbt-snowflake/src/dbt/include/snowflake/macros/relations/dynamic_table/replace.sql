{% macro snowflake__get_replace_dynamic_table_sql(relation, sql) -%}
{#-
    Produce DDL that replaces a dynamic table with a new dynamic table

    Args:
    - relation: Union[SnowflakeRelation, str]
        - SnowflakeRelation - required for relation.render()
        - str - is already the rendered relation name
    - sql: str - the code defining the model
    Globals:
    - config: NodeConfig - contains the attribution required to produce a SnowflakeDynamicTableConfig
    Returns:
        A valid DDL statement which will result in a new dynamic table.
-#}

    {%- set dynamic_table = relation.from_config(config.model) -%}
    {%- set catalog_integration = adapter.get_catalog_integration_from_model(config.model) -%}

    {%- if catalog is None -%}
        {{ snowflake__replace_dynamic_table_standard_sql(dynamic_table, relation, sql) }}
    {%- elif catalog_integration.catalog_type == 'iceberg_managed' -%}
        {{ snowflake__replace_dynamic_table_iceberg_managed_sql(dynamic_table, relation, catalog, sql) }}
    {%- endif -%}

{%- endmacro %}

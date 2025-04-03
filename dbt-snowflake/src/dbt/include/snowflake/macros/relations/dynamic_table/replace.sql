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
    {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

    {%- if catalog_relation.catalog_type == 'NATIVE' -%}
        {{ snowflake__replace_dynamic_table_standard_sql(dynamic_table, relation, sql) }}
    {%- elif catalog_relation.catalog_type == 'ICEBERG_MANAGED' -%}
        {{ snowflake__replace_dynamic_table_iceberg_managed_sql(dynamic_table, relation, sql) }}
    {%- else -%}
        {% do exceptions.raise_compiler_error('Unexpected model config for: ' ~ relation) %}
    {%- endif -%}

{%- endmacro %}

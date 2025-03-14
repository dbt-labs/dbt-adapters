{% macro snowflake__describe_dynamic_table(relation) %}
{#-
--  Get all relevant metadata about a dynamic table
--
--  Args:
--  - relation: SnowflakeRelation - the relation to describe
--  Returns:
--      A dictionary with one or two entries depending on whether iceberg is enabled:
--      - dynamic_table: the metadata associated with a standard dynamic table
--      - catalog: the metadata associated with the iceberg catalog
-#}
    {%- set _dynamic_table_sql -%}
        show dynamic tables
            like '{{ relation.identifier }}'
            in schema {{ relation.database }}.{{ relation.schema }}
        ;
        select
            "NAME" as "name",
            "SCHEMA_NAME" as "schema_name",
            "DATABASE_NAME" as "database_name",
            "TEXT" as "text",
            "TARGET_LAG" as "target_lag",
            "WAREHOUSE" as "warehouse",
            "REFRESH_MODE" as "refresh_mode"
        from table(result_scan(last_query_id()))
    {%- endset %}
    {% set results = {'dynamic_table': run_query(_dynamic_table_sql)} %}

    {% if adapter.behavior.enable_iceberg_materializations.no_warn %}
        {% set _ = results.update({'catalog': run_query(_get_describe_iceberg_catalog_sql(relation))}) %}
    {% endif %}

    {% do return(results) %}
{% endmacro %}


{% macro _get_describe_iceberg_catalog_sql(relation) %}
{#-
--  Produce DQL that returns all relevant metadata about an iceberg catalog
--
--  Args:
--  - relation: SnowflakeRelation - the relation to describe
--  Returns:
--      A valid DQL statement that will return metadata associated with an iceberg catalog
-#}
    show iceberg tables
        like '{{ relation.identifier }}'
        in schema {{ relation.database }}.{{ relation.schema }}
    ;
    select
        CATALOG_NAME as "catalog_name",
        EXTERNAL_VOLUME_NAME as "external_volume_name",
        BASE_LOCATION as "base_location"
    from table(result_scan(last_query_id()))
{% endmacro %}

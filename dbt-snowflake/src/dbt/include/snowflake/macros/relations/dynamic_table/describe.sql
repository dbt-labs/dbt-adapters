{% macro snowflake__describe_dynamic_table(relation) %}
{#-
    Get all relevant metadata about a dynamic table

    Args:
    - relation: SnowflakeRelation - the relation to describe
    Returns:
        A dictionary with one or two entries depending on whether iceberg is enabled:
        - dynamic_table: the metadata associated with an info schema dynamic table
-#}
    {%- set _dynamic_table_sql -%}
    alter session set quoted_identifiers_ignore_case = false;
    show dynamic tables
        like '{{ relation.identifier }}'
        in schema {{ relation.database }}.{{ relation.schema }}
    ;
    select
        "name",
        "schema_name",
        "database_name",
        "text",
        "target_lag",
        "warehouse",
        "refresh_mode"
    from table(result_scan(last_query_id()))
    ;
    {%- endset -%}

    {%- set results = {'dynamic_table': run_query(_dynamic_table_sql)} -%}

    alter session unset quoted_identifiers_ignore_case;

    {%- do return(results) -%}

{% endmacro %}

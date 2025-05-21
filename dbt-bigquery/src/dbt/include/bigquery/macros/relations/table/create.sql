{% macro bigquery__create_table_as(temporary, relation, compiled_code, language='sql') -%}
    {%- if language == 'sql' -%}
        {{ bigquery__create_table_info_schema_sql(temporary, relation, compiled_code) }}
    {%- elif language == 'python' -%}
        {#-
            N.B. Python models _can_ write to temp views HOWEVER they use a different session
            and have already expired by the time they need to be used (I.E. in merges for incremental models)

            TODO: Deep dive into spark sessions to see if we can reuse a single session for an entire dbt invocation.
        -#}

        {#- when a user wants to change the schema of an existing relation, they must intentionally drop the table in the dataset -#}
        {%- set old_relation = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) -%}
        {%- if (old_relation.is_table and (should_full_refresh())) -%}
            {%- do adapter.drop_relation(relation) -%}
        {%- endif -%}

        {%- set submission_method = config.get("submission_method", "serverless") -%}
        {%- if submission_method in ("serverless", "cluster") -%}
            {{ py_write_table(compiled_code=compiled_code, target_relation=relation.quote(database=False, schema=False, identifier=False)) }}
        {%- elif submission_method == "bigframes" -%}
            {{ bigframes_write_table(compiled_code=compiled_code, target_relation=relation.quote(database=False, schema=False, identifier=False)) }}
        {%- else -%}
            {%- do exceptions.raise_compiler_error("bigquery__create_table_as macro didn't get supported dataframe syntax, it got %s" % submission_method) -%}
        {%- endif -%}
    {%- else -%}
        {% do exceptions.raise_compiler_error("bigquery__create_table_as macro didn't get supported language, it got %s" % language) %}
    {%- endif -%}

{%- endmacro -%}


{% macro bigquery__create_table_info_schema_sql(temporary, relation, compiled_code) -%}
{#-
    Implements CREATE TABLE:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_statement
-#}
    {%- set raw_partition_by = config.get('partition_by', none) -%}
    {%- set raw_cluster_by = config.get('cluster_by', none) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}
    {%- if partition_config.time_ingestion_partitioning -%}
        {%- set columns = get_columns_with_types_in_query_sql(sql) -%}
        {%- set table_dest_columns_csv = columns_without_partition_fields_csv(partition_config, columns) -%}
        {%- set columns = '(' ~ table_dest_columns_csv ~ ')' -%}
    {%- endif -%}

    {%- set contract_config = config.get('contract') -%}
    {%- if contract_config.enforced -%}
        {{ get_assert_columns_equivalent(compiled_code) }}
        {%- set compiled_code = get_select_subquery(compiled_code) %}
    {% endif %}

{{ sql_header if sql_header is not none }}

create or replace table {{ relation }}
    {%- if contract_config.enforced -%}
    {{ get_table_columns_and_constraints() }}
    {% else %}
    {#-- cannot do contracts at the same time as time ingestion partitioning -#}
    {{ columns }}
    {% endif %}
    {{ partition_by(partition_config) }}
    {{ cluster_by(raw_cluster_by) }}
    {{ bigquery_table_options(config, model, temporary) }}
    {#-- PARTITION BY cannot be used with the AS query_statement clause.
         https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#partition_expression
    -#}
    {%- if not partition_config.time_ingestion_partitioning %}
    as (
        {{ compiled_code }}
    )
    {% endif %}
;

{%- endmacro -%}

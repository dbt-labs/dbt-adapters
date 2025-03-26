{#
    The Snowflake INSERT OVERWRITE instruction is not a partition‐targeted update like in Spark or
    the write disposition options in BigQuery. In Snowflake, specifying OVERWRITE causes the entire
    target table to be cleared (essentially a TRUNCATE) before the new data is inserted in one atomic
    operation. That means every time running an INSERT OVERWRITE means discarding all existing data
    and replacing it wholesale.

    But because dbt has very specific logic for dropping tables, we do not add this method as a table
    option. It would complicate that mental model tremendously. We instead pass this caveat about this
    so-called incremental strategy to the user.

    https://github.com/dbt-labs/dbt-adapters/issues/736#issuecomment-2640918081
#}

{% macro get_incremental_insert_overwrite_sql(arg_dict) -%}
  {{ adapter.dispatch('insert_overwrite_get_sql', 'dbt')(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["unique_key"], arg_dict["dest_columns"]) }}
{%- endmacro %}

{% macro snowflake__insert_overwrite_get_sql(target, source, unique_key, dest_columns) -%}

    {%- set dml -%}

    {%- set overwrite_columns = config.get('overwrite_columns', []) -%}

    {{ config.get('sql_header', '') }}

    {% set target_columns_list = '(' ~ ', '.join(overwrite_columns) ~ ')' if overwrite_columns else '' %}
    {% set source_query_columns_list = ', '.join(overwrite_columns) if overwrite_columns else '*' %}
    insert overwrite into {{ target.render() }} {{ target_columns_list }}
        select {{ source_query_columns_list }}
        from {{ source.render() }}

    {%- endset -%}

    {% do return(snowflake_dml_explicit_transaction(dml)) %}

{% endmacro %}

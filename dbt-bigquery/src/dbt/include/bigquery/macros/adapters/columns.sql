{% macro bigquery__get_columns_for_unit_tests(relation) -%}
  {{ return(adapter.get_columns_and_pseudocolumns_for_relation(relation)) }}
{%- endmacro %}

{% macro bigquery__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    {%- if select_sql_header is not none -%}
    {{ select_sql_header }}
    {%- endif -%}
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
    where false and current_timestamp() = current_timestamp()
    limit 0
{% endmacro %}

{% macro snowflake__list_relations_without_caching(schema_relation, max_iter=10, max_results_per_iter=10000) %}

    {%- if schema_relation is string -%}
        {%- set schema = schema_relation -%}
    {%- else -%}
        {%- set schema = schema_relation.include(identifier=False) -%}
    {%- endif -%}

    {%- set max_results_per_iter = adapter.config.flags.get('list_relations_per_page', max_results_per_iter) -%}
    {%- set max_iter = adapter.config.flags.get('list_relations_page_limit', max_iter) -%}
    {%- set too_many_relations_msg -%}
        dbt is currently configured to list a maximum of {{ max_results_per_iter * max_iter }} objects per schema.
        {{ schema }} exceeds this limit. If this is expected, you may configure this limit
        by setting list_relations_per_page and list_relations_page_limit in your project flags.
        It is recommended to start by increasing list_relations_page_limit to something more than the default of 10.
    {%- endset -%}

    {%- set paginated_results = [] -%}
    {%- set watermark = none -%}

    {%- do run_query('alter session set quoted_identifiers_ignore_case = false;') -%}

    {#- loop an extra time to catch the breach of max iterations -#}
    {%- for _ in range(0, max_iter + 1) -%}

        {#- raise an error if we still didn't exit and we're beyond the max iterations limit -#}
        {%- if loop.index == max_iter -%}
            {%- do exceptions.raise_compiler_error(too_many_relations_msg) -%}
        {%- endif -%}

        {%- set show_objects_sql = snowflake__show_objects_sql(schema, max_results_per_iter, watermark) -%}
        {%- set paginated_result = run_query(show_objects_sql) -%}
        {%- do paginated_results.append(paginated_result) -%}
        {%- set watermark = paginated_result.columns.get('name').values()[-1] -%}

        {#- we got less results than the max_results_per_iter (includes 0), meaning we reached the end -#}
        {%- if (paginated_result | length) < max_results_per_iter -%}
            {%- break -%}
        {%- endif -%}

    {%- endfor -%}

    {%- do run_query('alter session unset quoted_identifiers_ignore_case;') -%}

    {#- grab the first table in the paginated results to access the `merge` method -#}
    {%- set agate_table = paginated_results[0] -%}
    {%- do return(agate_table.merge(paginated_results)) -%}

{% endmacro %}


{% macro snowflake__show_objects_sql(schema, max_results_per_iter=10000, watermark=none) %}

{%- set _sql -%}
show objects in {{ schema }}
    limit {{ max_results_per_iter }}
    {% if watermark is not none -%} from '{{ watermark }}' {%- endif %}
;

{#- gated for performance reasons - if you don't want iceberg, you shouldn't pay the latency penalty -#}
{%- if adapter.behavior.enable_iceberg_materializations.no_warn %}
select all_objects.*, all_tables.IS_ICEBERG as "is_iceberg"
from table(result_scan(last_query_id(-1))) all_objects
left join {{ schema.database }}.INFORMATION_SCHEMA.tables as all_tables
on all_tables.table_name = all_objects."name"
and all_tables.table_schema = all_objects."schema_name"
and all_tables.table_catalog = all_objects."database_name"
show iceberg tables in {{ schema }};

select all_objects.*, iceberg_objects."catalog_name"
from table(result_scan(last_query_id(-2))) all_objects
left join table(result_scan(last_query_id(-1))) iceberg_objects
on iceberg_objects."name" = all_objects."name"
and iceberg_objects."schema_name" = all_objects."schema_name"
and iceberg_objects."database_name" = all_objects."database_name"
;
{%- endif -%}

{%- endset -%}

{%- do return(_sql) -%}

{% endmacro %}

{% macro redshift__describe_materialized_view(relation) %}
    {#-
        These need to be separate queries because redshift will not let you run queries
        against svv_table_info and pg_views in the same query.

        Uses SHOW COLUMNS for column descriptors to avoid OID-based catalog queries
        that can fail with "could not open relation with OID" errors.
    -#}

    {%- set _materialized_view_sql -%}
        select
            tb.database,
            tb.schema,
            tb.table,
            tb.diststyle,
            tb.sortkey1,
            mv.autorefresh
        from svv_table_info tb
        -- svv_mv_info is queryable by Redshift Serverless, but stv_mv_info is not
        left join svv_mv_info mv
            on mv.database_name = tb.database
            and mv.schema_name = tb.schema
            and mv.name = tb.table
        where tb.table ilike '{{ relation.identifier }}'
        and tb.schema ilike '{{ relation.schema }}'
        and tb.database ilike '{{ relation.database }}'
    {%- endset %}
    {% set _materialized_view = run_query(_materialized_view_sql) %}

    {#-
        SHOW COLUMNS returns dist_key (1 if distribution key, empty otherwise)
        and sort_key_order (position in sort key, 0 if not part of sort key).
        This replaces the OID-based pg_class/pg_attribute query.
    -#}
    {%- set _column_descriptor_sql -%}
        select
            column_name as column,
            case when dist_key = 1 then true else false end as is_dist_key,
            sort_key_order as sort_key_position
        from (
            SHOW COLUMNS FROM TABLE {{ relation.database }}.{{ relation.schema }}.{{ relation.identifier }}
        )
    {%- endset %}
    {% set _column_descriptor = run_query(_column_descriptor_sql) %}

    {%- set _query_sql -%}
        select
            vw.definition
        from pg_views vw
        where vw.viewname = '{{ relation.identifier }}'
        and vw.schemaname = '{{ relation.schema }}'
        and vw.definition ilike '%create materialized view%'
    {%- endset %}
    {% set _query = run_query(_query_sql) %}

    {% do return({
       'materialized_view': _materialized_view,
       'query': _query,
       'columns': _column_descriptor,
    })%}

{% endmacro %}

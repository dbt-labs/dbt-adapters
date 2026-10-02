{% macro redshift__describe_materialized_view(relation) %}
    {#-
        These need to be separate queries because redshift will not let you run queries
        against svv_table_info and pg_views in the same query. The same is true of svv_redshift_columns.
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
        Query the internal MV storage table (mv_tbl__<name>__*) for distkey/sortkey info.
        Redshift stores MV data in this internal table, so we need to query pg_attribute
        on this table to get the distribution and sort key configuration.
    -#}
    {%- set _column_descriptor_sql -%}
              select
            r.column_name as column,
            a.attisdistkey as is_dist_key,
            a.attsortkeyord as sort_key_position
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        join pg_attribute a on a.attrelid = c.oid
        join svv_redshift_columns r on r.ordinal_position = a.attnum
        where
            n.nspname ilike '{{ relation.schema }}'
            and c.relname like 'mv_tbl__{{ relation.identifier }}__%'
            and r.schema_name = '{{ relation.schema }}'
            and r.table_name = '{{ relation.identifier }}'
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

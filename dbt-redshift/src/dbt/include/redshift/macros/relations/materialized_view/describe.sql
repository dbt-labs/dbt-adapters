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
        SELECT
            a.attname as column,
            a.attisdistkey as is_dist_key,
            a.attsortkeyord as sort_key_position
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE
            n.nspname ilike '{{ relation.schema }}'
            AND c.relname LIKE 'mv_tbl__{{ relation.identifier }}__%'
    {%- endset %}
    {% set _column_descriptor = run_query(_column_descriptor_sql) %}

    {#-
        Redshift Patch 206 moved the internal backing table out of the MV's schema, so the query above
        returns nothing. Fall back to the first sort key on svv_table_info, which is keyed by the MV itself.
        svv_table_info only exposes the first sort column, so this is limited to single-column keys
        (sortkey_num = 1). A multi-column key returns nothing here, compares as changed and triggers
        a full refresh rather than being mistaken for a single-column key.
    -#}
    {% if _column_descriptor | length == 0 %}
        {%- set _sort_fallback_sql -%}
            select
                tb.sortkey1 as column,
                false as is_dist_key,
                1 as sort_key_position
            from svv_table_info tb
            where tb.table ilike '{{ relation.identifier }}'
            and tb.schema ilike '{{ relation.schema }}'
            and tb.database ilike '{{ relation.database }}'
            and tb.sortkey_num = 1
            and tb.sortkey1 is not null
            and tb.sortkey1 not in ('', 'AUTO(SORTKEY)', 'INTERLEAVED')
        {%- endset %}
        {% set _column_descriptor = run_query(_sort_fallback_sql) %}
    {% endif %}

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

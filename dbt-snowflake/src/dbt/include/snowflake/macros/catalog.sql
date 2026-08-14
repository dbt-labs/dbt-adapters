{% macro snowflake__get_catalog(information_schema, schemas) -%}

    {% set query %}
        with tables as (
            {{ snowflake__catalog_tables_by_schemas_sql(information_schema, schemas) }}
        ),
        columns as (
            {{ snowflake__catalog_columns_by_schemas_sql(information_schema, schemas) }}
        )
        {{ snowflake__get_catalog_results_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}

{%- endmacro %}


{% macro snowflake__get_catalog_relations(information_schema, relations) -%}

    {% set query %}
        with tables as (
            {{ snowflake__catalog_tables_by_relations_sql(information_schema, relations) }}
        ),
        columns as (
            {{ snowflake__catalog_columns_by_relations_sql(information_schema, relations) }}
        )
        {{ snowflake__get_catalog_results_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}

{%- endmacro %}


{% macro snowflake__get_catalog_tables_sql(information_schema, source_sql=none) -%}
    select
        table_catalog as "table_database",
        table_schema as "table_schema",
        table_name as "table_name",
        case
            when is_dynamic = 'YES' and table_type = 'BASE TABLE' THEN 'DYNAMIC TABLE'
            else table_type
        end as "table_type",
        comment as "table_comment",

        -- note: this is the _role_ that owns the table
        table_owner as "table_owner",

        'Clustering Key' as "stats:clustering_key:label",
        clustering_key as "stats:clustering_key:value",
        'The key used to cluster this table' as "stats:clustering_key:description",
        (clustering_key is not null) as "stats:clustering_key:include",

        'Row Count' as "stats:row_count:label",
        row_count as "stats:row_count:value",
        'An approximate count of rows in this table' as "stats:row_count:description",
        (row_count is not null) as "stats:row_count:include",

        'Approximate Size' as "stats:bytes:label",
        bytes as "stats:bytes:value",
        'Approximate size of the table as reported by Snowflake' as "stats:bytes:description",
        (bytes is not null) as "stats:bytes:include",

        'Last Modified' as "stats:last_modified:label",
        to_varchar(convert_timezone('UTC', last_altered), 'yyyy-mm-dd HH24:MI'||'UTC') as "stats:last_modified:value",
        'The timestamp for last update/change' as "stats:last_modified:description",
        (last_altered is not null and table_type='BASE TABLE') as "stats:last_modified:include"
    from {{ source_sql if source_sql is not none else information_schema ~ '.tables' }}
{%- endmacro %}


{% macro snowflake__get_catalog_columns_sql(information_schema, source_sql=none) -%}
    select
        table_catalog as "table_database",
        table_schema as "table_schema",
        table_name as "table_name",

        column_name as "column_name",
        ordinal_position as "column_index",
        data_type as "column_type",
        comment as "column_comment"
    from {{ source_sql if source_sql is not none else information_schema ~ '.columns' }}
{%- endmacro %}


{% macro snowflake__get_catalog_results_sql() -%}
    select *
    from tables
    join columns using ("table_database", "table_schema", "table_name")
    order by "column_index"
{%- endmacro %}


{#
    `quote` picks which spelling of `field` the predicate refers to.

    Quoted is correct on the outer select, where the projection has aliased the column to a
    quoted lowercase name (`table_schema as "table_schema"`) that Snowflake lets a where clause
    reference. It is *not* correct directly against an information_schema view, whose real
    columns are uppercase -- `"table_schema"` raises `invalid identifier` there. Pass
    `quote=false` whenever the predicate sits on the view rather than on the projection.
#}
{% macro snowflake__catalog_equals(field, value, quote=true) %}
    {%- set column = '"' ~ field ~ '"' if quote else field -%}
    {{ column }} ilike '{{ value }}' and upper({{ column }}) = upper('{{ value }}')
{% endmacro %}


{% macro snowflake__catalog_tables_by_schemas_sql(information_schema, schemas) -%}
    {%- if adapter.behavior.snowflake_catalog_scan_per_schema.no_warn -%}
        {{ snowflake__get_catalog_tables_sql(
            information_schema,
            snowflake__pruned_catalog_scan_by_schemas_sql(information_schema, 'tables', schemas)) }}
    {%- else %}
        {{ snowflake__get_catalog_tables_sql(information_schema) }}
        {{ snowflake__get_catalog_schemas_where_clause_sql(schemas) }}
    {%- endif -%}
{%- endmacro %}


{% macro snowflake__catalog_columns_by_schemas_sql(information_schema, schemas) -%}
    {%- if adapter.behavior.snowflake_catalog_scan_per_schema.no_warn -%}
        {{ snowflake__get_catalog_columns_sql(
            information_schema,
            snowflake__pruned_catalog_scan_by_schemas_sql(information_schema, 'columns', schemas)) }}
    {%- else %}
        {{ snowflake__get_catalog_columns_sql(information_schema) }}
        {{ snowflake__get_catalog_schemas_where_clause_sql(schemas) }}
    {%- endif -%}
{%- endmacro %}


{% macro snowflake__catalog_tables_by_relations_sql(information_schema, relations) -%}
    {%- if adapter.behavior.snowflake_catalog_scan_per_schema.no_warn -%}
        {{ snowflake__get_catalog_tables_sql(
            information_schema,
            snowflake__pruned_catalog_scan_by_relations_sql(information_schema, 'tables', relations)) }}
    {%- else %}
        {{ snowflake__get_catalog_tables_sql(information_schema) }}
        {{ snowflake__get_catalog_relations_where_clause_sql(relations) }}
    {%- endif -%}
{%- endmacro %}


{% macro snowflake__catalog_columns_by_relations_sql(information_schema, relations) -%}
    {%- if adapter.behavior.snowflake_catalog_scan_per_schema.no_warn -%}
        {{ snowflake__get_catalog_columns_sql(
            information_schema,
            snowflake__pruned_catalog_scan_by_relations_sql(information_schema, 'columns', relations)) }}
    {%- else %}
        {{ snowflake__get_catalog_columns_sql(information_schema) }}
        {{ snowflake__get_catalog_relations_where_clause_sql(relations) }}
    {%- endif -%}
{%- endmacro %}


{#
    Snowflake only takes the fast per-schema metadata path when the schema filter is a
    single equality. An `or` across schemas drops it back to materializing the whole
    database's metadata, so scan one schema at a time and union the results instead.

    Only the scan is repeated per schema, never the surrounding projection: the projection
    for `tables` is ~1.5kB and Snowflake caps a statement at 1MB, so inlining it per schema
    would fail outright on a database with a few hundred schemas -- exactly the case this
    flag exists to speed up. The projection is applied once, over the union.
#}
{% macro snowflake__pruned_catalog_scan_by_schemas_sql(information_schema, view_name, schemas) -%}
    (
        {%- for schema in schemas | map('upper') | unique | sort %}
        select * from {{ information_schema }}.{{ view_name }}
        {{ snowflake__get_catalog_schemas_where_clause_sql([schema], quote=false) }}
        {%- if not loop.last %}
        union all
        {%- endif %}
        {%- endfor %}
    ) as pruned_{{ view_name }}
{%- endmacro %}


{#
    As above, but grouping the requested relations by schema so that each scan is still
    filtered to a single schema.
#}
{% macro snowflake__pruned_catalog_scan_by_relations_sql(information_schema, view_name, relations) -%}
    {%- set schema_groups = {} -%}
    {%- for relation in relations -%}
        {%- set schema = (relation.schema or '') | upper -%}
        {%- if schema not in schema_groups -%}
            {%- do schema_groups.update({schema: []}) -%}
        {%- endif -%}
        {%- do schema_groups[schema].append(relation) -%}
    {%- endfor -%}

    (
        {%- for schema, schema_relations in schema_groups | dictsort %}
        select * from {{ information_schema }}.{{ view_name }}
        {{ snowflake__pruned_catalog_relations_where_clause_sql(schema_relations) }}
        {%- if not loop.last %}
        union all
        {%- endif %}
        {%- endfor %}
    ) as pruned_{{ view_name }}
{%- endmacro %}


{#
    `relations` must all belong to the same schema -- mixed schemas raise rather than silently
    reporting on only the first one. The schema match is hoisted out of the identifier
    disjunction so that it stays a single equality conjunct.
#}
{% macro snowflake__pruned_catalog_relations_where_clause_sql(relations) -%}
    {%- if relations | rejectattr('schema') | list %}
        {%- do exceptions.raise_compiler_error(
            '`get_catalog_relations` requires a list of relations, each with a schema'
        ) %}
    {%- endif %}

    {%- set schemas = relations | map(attribute='schema') | map('upper') | unique | sort -%}
    {%- if schemas | length > 1 %}
        {%- do exceptions.raise_compiler_error(
            '`snowflake__pruned_catalog_relations_where_clause_sql` requires relations from a'
            ~ ' single schema, got: ' ~ (schemas | join(', '))
        ) %}
    {%- endif %}

    {%- set whole_schema = relations | rejectattr('identifier') | list | length > 0 -%}

    where {{ snowflake__catalog_equals('table_schema', relations[0].schema, quote=false) }}
    {%- if not whole_schema %}
        and ({%- for identifier in relations | map(attribute='identifier') | unique | sort -%}
            ({{ snowflake__catalog_equals('table_name', identifier, quote=false) }}){%- if not loop.last %} or {% endif -%}
        {%- endfor -%})
    {%- endif %}
{%- endmacro %}


{% macro snowflake__get_catalog_schemas_where_clause_sql(schemas, quote=true) -%}
    where ({%- for schema in schemas -%}
        ({{ snowflake__catalog_equals('table_schema', schema, quote) }}){%- if not loop.last %} or {% endif -%}
    {%- endfor -%})
{%- endmacro %}


{% macro snowflake__get_catalog_relations_where_clause_sql(relations) -%}
    where (
        {%- for relation in relations -%}
            {% if relation.schema and relation.identifier %}
                (
                    {{ snowflake__catalog_equals('table_schema', relation.schema) }}
                    and {{ snowflake__catalog_equals('table_name', relation.identifier) }}
                )
            {% elif relation.schema %}
                (
                    {{ snowflake__catalog_equals('table_schema', relation.schema) }}
                )
            {% else %}
                {% do exceptions.raise_compiler_error(
                    '`get_catalog_relations` requires a list of relations, each with a schema'
                ) %}
            {% endif %}

            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}

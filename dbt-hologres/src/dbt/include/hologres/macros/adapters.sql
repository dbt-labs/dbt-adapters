{% macro hologres__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if language == 'sql' -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    create {% if temporary -%}
      temporary
    {%- endif %} table {{ relation }}
    as (
      {{ compiled_code }}
    );
  {%- elif language == 'python' -%}
    {{ py_write_table(compiled_code=compiled_code, target_relation=relation, temporary=temporary) }}
  {%- else -%}
    {% do exceptions.raise_compiler_error("hologres__create_table_as macro didn't get supported language " ~ language) %}
  {%- endif -%}
{%- endmacro %}

{% macro hologres__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create view {{ relation }} as (
    {{ sql }}
  );
{%- endmacro %}

{% macro hologres__get_catalog(information_schema, schemas) -%}
  {%- call statement('catalog', fetch_result=True) -%}
    select
        '{{ database }}' as table_database,
        sch.nspname as table_schema,
        tbl.relname as table_name,
        case tbl.relkind
            when 'v' then 'VIEW'
            when 'r' then 'BASE TABLE'
        end as table_type,
        tbl_desc.description as table_comment,
        col.attname as column_name,
        col.attnum as column_index,
        pg_catalog.format_type(col.atttypid, col.atttypmod) as column_type,
        col_desc.description as column_comment,
        pg_catalog.col_description(tbl.oid, col.attnum) as column_description,
        '' as table_owner
    from pg_catalog.pg_class tbl
    inner join pg_catalog.pg_namespace sch on tbl.relnamespace = sch.oid
    inner join pg_catalog.pg_attribute col on col.attrelid = tbl.oid
    left outer join pg_catalog.pg_description tbl_desc on (
            tbl_desc.objoid = tbl.oid and tbl_desc.objsubid = 0
    )
    left outer join pg_catalog.pg_description col_desc on (
            col_desc.objoid = tbl.oid and col_desc.objsubid = col.attnum
    )
    where (
        {%- for schema in schemas -%}
          upper(sch.nspname) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
      and tbl.relkind in ('r', 'v', 'm', 'f', 'p')
      and col.attnum > 0
      and not col.attisdropped
    order by sch.nspname, tbl.relname, col.attnum
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}

{% macro hologres__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database }}' as database,
      tablename as name,
      schemaname as schema,
      'table' as type
    from pg_tables
    where schemaname ilike '{{ schema_relation.schema }}'
    union all
    select
      '{{ schema_relation.database }}' as database,
      viewname as name,
      schemaname as schema,
      'view' as type
    from pg_views
    where schemaname ilike '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro hologres__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select distinct nspname from pg_namespace
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro hologres__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
        select count(*) from pg_namespace where nspname = '{{ schema }}'
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro hologres__create_schema(relation) -%}
  {% call statement('create_schema') %}
    create schema if not exists {{ relation.without_identifier().include(database=False) }}
  {% endcall %}
{% endmacro %}

{% macro hologres__drop_schema(relation) -%}
  {% call statement('drop_schema') %}
    drop schema if exists {{ relation.without_identifier().include(database=False) }} cascade
  {% endcall %}
{% endmacro %}

{% macro hologres__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }} cascade
  {%- endcall %}
{% endmacro %}

{% macro hologres__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro hologres__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}

{% macro hologres__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale
      from information_schema.columns
      where table_name = '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema = '{{ relation.schema }}'
        {% endif %}
      order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro hologres__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix ~ py_current_timestring() %}
    {% set tmp_relation = base_relation.incorporate(path={"identifier": tmp_identifier}) -%}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro hologres__get_relations() %}
  {# Hologres-specific macro to get relation dependencies #}
  {% call statement('get_relations', fetch_result=True) %}
      select
          dependent_ns.nspname as dependent_schema,
          dependent_view.relname as dependent_name,
          source_ns.nspname as referenced_schema,
          source_table.relname as referenced_name
      from pg_depend
      join pg_rewrite on pg_depend.objid = pg_rewrite.oid
      join pg_class as dependent_view on pg_rewrite.ev_class = dependent_view.oid
      join pg_class as source_table on pg_depend.refobjid = source_table.oid
      join pg_namespace dependent_ns on dependent_ns.oid = dependent_view.relnamespace
      join pg_namespace source_ns on source_ns.oid = source_table.relnamespace
      where dependent_view.relkind = 'v'
        and source_table.relkind = 'r'
        and pg_depend.deptype = 'n'
        and dependent_ns.nspname != source_ns.nspname
  {% endcall %}

  {% do return(load_result('get_relations').table) %}
{% endmacro %}

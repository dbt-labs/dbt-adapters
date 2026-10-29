{#-
  Schema and relation-listing metadata for lake compute.

  The driver implements none of the ADBC metadata API, so every lookup is SQL against
  `system.information_schema.schemata` / `.tables`.
-#}


{% macro lakecompute__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier().render() }}
  {%- endcall -%}
{% endmacro %}


{% macro lakecompute__drop_schema(relation) -%}
  {#-
    `drop schema ... cascade` is not supported for Iceberg schemas (even with `if exists` on an
    absent schema), and a bare `drop schema` over a populated schema fails in Polaris. So drop
    every relation, then the empty schema, as one `;`-joined request. Views go first so no
    table is dropped while a view still references it.
  -#}
  {%- set schema_relation = relation.without_identifier() -%}
  {%- set relations = list_relations_without_caching(schema_relation) -%}

  {%- set views = [] -%}
  {%- set tables = [] -%}
  {%- for row in relations -%}
    {%- if row['type'] == 'view' -%}
      {%- do views.append(row['name']) -%}
    {%- else -%}
      {%- do tables.append(row['name']) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set statements = [] -%}
  {%- for name in views -%}
    {%- do statements.append('drop view if exists ' ~ schema_relation.render() ~ '.' ~ name) -%}
  {%- endfor -%}
  {%- for name in tables -%}
    {%- do statements.append('drop table if exists ' ~ schema_relation.render() ~ '.' ~ name) -%}
  {%- endfor -%}
  {%- do statements.append('drop schema if exists ' ~ schema_relation.render()) -%}

  {%- call statement('drop_schema') -%}
    {{ statements | join(';\n') }}
  {%- endcall -%}
{% endmacro %}


{% macro lakecompute__list_schemas(database) -%}
  {% set sql %}
    select schema_name
    from system.information_schema.schemata
    {% if database is not none %}
    where lower(catalog_name) = '{{ database | lower }}'
    {% endif %}
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}


{% macro lakecompute__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
        select count(*)
        from system.information_schema.schemata
        where lower(schema_name) = '{{ schema | lower }}'
        and lower(catalog_name) = '{{ information_schema.database | lower }}'
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}


{% macro lakecompute__list_relations_without_caching(schema_relation) -%}
  {#-
    Column order is the contract: `LakeComputeAdapter.list_relations_without_caching` unpacks
    rows positionally. An unrecognised `table_type` yields NULL, which becomes `External`.
  -#}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database }}' as database,
      table_name as name,
      table_schema as schema,
      CASE table_type
        WHEN 'BASE TABLE' THEN 'table'
        WHEN 'VIEW' THEN 'view'
        WHEN 'LOCAL TEMPORARY' THEN 'table'
        END as type
    from system.information_schema.tables
    where lower(table_schema) = '{{ schema_relation.schema | lower }}'
    and lower(table_catalog) = '{{ schema_relation.database | lower }}'
  {%- endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro bigquery__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if language == 'sql' -%}
    {%- set raw_partition_by = config.get('partition_by', none) -%}
    {%- set raw_cluster_by = config.get('cluster_by', none) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}
    {%- if partition_config.time_ingestion_partitioning -%}
    {%- set columns = get_columns_with_types_in_query_sql(sql) -%}
    {%- set table_dest_columns_csv = columns_without_partition_fields_csv(partition_config, columns) -%}
    {%- set columns = '(' ~ table_dest_columns_csv ~ ')' -%}
    {%- endif -%}

    {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

    {{ sql_header if sql_header is not none }}

    create or replace table {{ relation }}
      {%- set contract_config = config.get('contract') -%}
      {%- if contract_config.enforced -%}
        {{ get_assert_columns_equivalent(compiled_code) }}
        {{ get_table_columns_and_constraints() }}
        {%- set compiled_code = get_select_subquery(compiled_code) %}
      {% else %}
        {#-- cannot do contracts at the same time as time ingestion partitioning -#}
        {{ columns }}
      {% endif %}
    {{ partition_by(partition_config) }}
    {{ cluster_by(raw_cluster_by) }}

    {% if catalog_relation.table_format == 'iceberg' and not temporary %}with connection default{% endif %}
    {{ bigquery_table_options(config, model, temporary) }}

    {#-- PARTITION BY cannot be used with the AS query_statement clause.
         https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#partition_expression
    -#}
    {%- if not partition_config.time_ingestion_partitioning %}
    as (
      {{ compiled_code }}
    );
    {%- endif %}
  {%- elif language == 'python' -%}
    {#--
    N.B. Python models _can_ write to temp views HOWEVER they use a different session
    and have already expired by the time they need to be used (I.E. in merges for incremental models)

    TODO: Deep dive into spark sessions to see if we can reuse a single session for an entire
    dbt invocation.
     --#}

    {#-- when a user wants to change the schema of an existing relation, they must intentionally drop the table in the dataset --#}
    {%- set old_relation = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) -%}
    {%- if (old_relation.is_table and (should_full_refresh())) -%}
      {% do adapter.drop_relation(relation) %}
    {%- endif -%}
    {%- set submission_method = config.get("submission_method", "serverless") -%}
    {%- if submission_method in ("serverless", "cluster") -%}
      {{ py_write_table(compiled_code=compiled_code, target_relation=relation.quote(database=False, schema=False, identifier=False)) }}
    {%- elif submission_method == "bigframes" -%}
      {{ bigframes_write_table(compiled_code=compiled_code, target_relation=relation.quote(database=False, schema=False, identifier=False)) }}
    {%- else -%}
      {% do exceptions.raise_compiler_error("bigquery__create_table_as macro didn't get supported dataframe syntax, it got %s" % submission_method) %} {%- endif -%}
  {%- else -%}
    {% do exceptions.raise_compiler_error("bigquery__create_table_as macro didn't get supported language, it got %s" % language) %}
  {%- endif -%}

{%- endmacro -%}

{% macro bigquery__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create or replace view {{ relation }}
  {{ bigquery_view_options(config, model) }}
  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %}
  as {{ sql }};

{% endmacro %}

{% macro bigquery__drop_schema(relation) -%}
  {{ adapter.drop_schema(relation) }}
{% endmacro %}

{% macro bigquery__get_columns_in_relation(relation) -%}
  {{ return(adapter.get_columns_in_relation(relation)) }}
{% endmacro %}


{% macro bigquery__list_relations_without_caching(schema_relation) -%}
  {{ return(adapter.list_relations_without_caching(schema_relation)) }}
{%- endmacro %}


{% macro bigquery__list_schemas(database) -%}
  {{ return(adapter.list_schemas(database)) }}
{% endmacro %}


{% macro bigquery__check_schema_exists(information_schema, schema) %}
  {{ return(adapter.check_schema_exists(information_schema.database, schema)) }}
{% endmacro %}

{#-- Handle both relation and column level documentation #}
{% macro bigquery__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_relation and config.persist_relation_docs() and model.description %}
    {% do alter_relation_comment(relation, model.description) %}
  {% endif %}
  {% if for_columns and config.persist_column_docs() and model.columns %}
    {% do alter_column_comment(relation, model.columns) %}
  {% endif %}
{% endmacro %}

{% macro bigquery__alter_relation_comment(relation, relation_comment) -%}
  {% do adapter.update_table_description(relation.database, relation.schema, relation.identifier, relation_comment) %}
{% endmacro %}

{% macro bigquery__alter_column_comment(relation, column_dict) -%}
  {% do adapter.update_columns(relation, column_dict) %}
{% endmacro %}

{% macro bigquery__alter_relation_add_columns(relation, add_columns) %}

  {% set sql -%}

     alter {{ relation.type }} {{ relation }}
        {% for column in add_columns %}
          add column {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
        {% endfor %}

  {%- endset -%}

  {{ return(run_query(sql)) }}

{% endmacro %}

{% macro bigquery__alter_relation_drop_columns(relation, drop_columns) %}

  {% set sql -%}

     alter {{ relation.type }} {{ relation }}

        {% for column in drop_columns %}
          drop column {{ column.name }}{{ ',' if not loop.last }}
        {% endfor %}

  {%- endset -%}

  {{ return(run_query(sql)) }}

{% endmacro %}


{% macro bigquery__alter_column_type(relation, column_name, new_column_type) -%}
  {#-- Changing a column's data type using a query requires you to scan the entire table.
    The query charges can be significant if the table is very large.

    https://cloud.google.com/bigquery/docs/manually-changing-schemas#changing_a_columns_data_type
  #}
  {% set relation_columns = get_columns_in_relation(relation) %}

  {% set sql %}
    select
      {%- for col in relation_columns -%}
        {% if col.column == column_name %}
          CAST({{ col.quoted }} AS {{ new_column_type }}) AS {{ col.quoted }}
        {%- else %}
          {{ col.quoted }}
        {%- endif %}
        {%- if not loop.last %},{% endif -%}
      {%- endfor %}
    from {{ relation }}
  {% endset %}

  {% call statement('alter_column_type') %}
    {{ create_table_as(False, relation, sql)}}
  {%- endcall %}

{% endmacro %}


{% macro bigquery__test_unique(model, column_name) %}

with dbt_test__target as (

  select {{ column_name }} as unique_field
  from {{ model }}
  where {{ column_name }} is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1

{% endmacro %}

{% macro bigquery__upload_file(local_file_path, database, table_schema, table_name) %}

  {{ log("kwargs: " ~ kwargs) }}

  {% do adapter.upload_file(local_file_path, database, table_schema, table_name, kwargs=kwargs) %}

{% endmacro %}

{% macro bigquery__make_relation_with_suffix(base_relation, suffix, dstring) %}
    {% if dstring %}
      {% set dt = modules.datetime.datetime.now() %}
      {% set dtstring = dt.strftime("%H%M%S%f") %}
      {% set suffix = suffix ~ dtstring %}
    {% endif %}
    {% set suffix_length = suffix|length %}
    {% set relation_max_name_length = 1024 %}  {# BigQuery limit #}
    {% if suffix_length > relation_max_name_length %}
        {% do exceptions.raise_compiler_error('Relation suffix is too long (' ~ suffix_length ~ ' characters). Maximum length is ' ~ relation_max_name_length ~ ' characters.') %}
    {% endif %}
    {% set identifier = base_relation.identifier[:relation_max_name_length - suffix_length] ~ suffix %}

    {{ return(base_relation.incorporate(path={"identifier": identifier })) }}

{% endmacro %}

{% macro bigquery__make_temp_relation(base_relation, suffix) %}
    {% set temp_relation = bigquery__make_relation_with_suffix(base_relation, suffix, dstring=True) %}
    {{ return(temp_relation) }}
{% endmacro %}

{% macro bigquery__make_intermediate_relation(base_relation, suffix) %}
    {{ return(bigquery__make_relation_with_suffix(base_relation, suffix, dstring=False)) }}
{% endmacro %}

{% macro bigquery__make_backup_relation(base_relation, backup_relation_type, suffix) %}
    {% set backup_relation = bigquery__make_relation_with_suffix(base_relation, suffix, dstring=False) %}
    {{ return(backup_relation.incorporate(type=backup_relation_type)) }}
{% endmacro %}

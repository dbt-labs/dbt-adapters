{% materialization incremental, adapter='spark', supported_languages=['sql', 'python'] -%}
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set raw_strategy = config.get('incremental_strategy') or 'append' -%}
  {%- set grant_config = config.get('grants') -%}

  {%- set file_format = dbt_spark_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {#-- Set vars --#}

  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set language = model['language'] -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) -%}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  {% set tmp_relation = this.incorporate(path = {"identifier": this.identifier ~ '__dbt_tmp'}) -%}

  {#-- for SQL model we will create temp view that doesn't have database and schema --#}
  {%- if language == 'sql'-%}
    {%- set tmp_relation = tmp_relation.include(database=false, schema=false) -%}
  {%- endif -%}

  {#-- Set Overwrite Mode --#}
  {%- if strategy in ['insert_overwrite', 'microbatch'] and partition_by -%}
    {%- call statement() -%}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {%- endcall -%}
  {%- endif -%}

  {#-- Run pre-hooks --#}
  {{ run_hooks(pre_hooks) }}

  {#-- Incremental run logic --#}
  {%- if existing_relation is none -%}
    {#-- Relation must be created --#}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}
    {% do persist_constraints(target_relation, model) %}
  {%- elif existing_relation.is_view or should_full_refresh() -%}
    {#-- Relation must be dropped & recreated --#}
    {% set is_delta = (file_format == 'delta' and existing_relation.is_delta) %}
    {% if not is_delta %} {#-- If Delta, we will `create or replace` below, so no need to drop --#}
      {% do adapter.drop_relation(existing_relation) %}
    {% endif %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}
    {% do persist_constraints(target_relation, model) %}
  {%- else -%}
    {#-- Relation must be merged: databricks implementation uses inline SQL, can't be used for Spark submit --#}
    {%- set submission_method = get_submission_method() -%}
    {% if language == 'python' and submission_method == 'spark_master' %}
      {%- call statement('main', language=language) %}
        {{ py_incremental(strategy, tmp_relation, target_relation, compiled_code, unique_key) }}
      {%- endcall -%}
    {% else %}
    {%- call statement('create_tmp_relation', language=language) -%}
      {{ create_table_as(True, tmp_relation, compiled_code, language) }}
    {%- endcall -%}
    {%- do process_schema_changes(on_schema_change, tmp_relation, existing_relation) -%}
    {%- call statement('main') -%}
      {{ dbt_spark_get_incremental_sql(strategy, tmp_relation, target_relation, existing_relation, unique_key, incremental_predicates) }}
    {%- endcall -%}
    {%- if language == 'python' -%}
      {#--
      This is yucky.
      See note in dbt-spark/dbt/include/spark/macros/adapters.sql
      re: python models and temporary views.

      Also, why do neither drop_relation or adapter.drop_relation work here?!
      --#}
      {% call statement('drop_relation') -%}
        drop table if exists {{ tmp_relation }}
      {%- endcall %}
    {%- endif -%}
    {%- endif -%}
  {%- endif -%}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

{% macro py_incremental(strategy, tmp_relation, target_relation, compiled_code, unique_key) %}
    {% if strategy == 'append' %}
        {{ py_incremental_append(tmp_relation, target_relation, compiled_code) }}
    {% elif strategy == 'merge' %}
        {{ py_incremental_merge(tmp_relation, target_relation, compiled_code, unique_key) }}
    {% else %}
        {% do exceptions.raise_compiler_error("Python incremental strategy '" ~ strategy ~ "' is not implemented.") %}
    {% endif %}
{% endmacro %}

{% macro py_incremental_append(tmp_relation, target_relation, compiled_code) %}
{{ log("Running Python incremental append strategy", info=True) }}

{% set language = 'python' %}

import sys
location = sys.argv[1]

{{ compiled_code }}

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

dbt = dbtObj(spark.table)

df = model(dbt, spark)

location_root = "{{ config.get('location_root', '') }}"
table_location = location_root.rstrip("/") + "/" + "{{ model["alias"] }}"
file_format = "{{ config.get('file_format', 'delta') }}"
write_options = {{ config.get('write_options', {}) }}

if file_format in ('delta', 'hudi', 'iceberg'):
    print(f"Appending to {file_format} table '{{ target_relation }}'")
    writer = df.write.mode("append").format(file_format)
    {% for key, value in config.get('write_options', {}).items() %}
    writer = writer.option("{{ key }}", "{{ value }}")
    {% endfor %}
    writer.saveAsTable("{{ target_relation }}")
else:
    print(f"Appending to path {table_location} as {file_format}")
    writer = df.write.mode("append").format(file_format)
    {% for key, value in config.get('write_options', {}).items() %}
    writer = writer.option("{{ key }}", "{{ value }}")
    {% endfor %}
    writer.save(table_location)
{% endmacro %}

{% macro py_incremental_merge(tmp_relation, target_relation, compiled_code, unique_key) %}
{{ log("Running Python incremental merge strategy", info=True) }}

{% set language = 'python' %}

import sys
location = sys.argv[1]

{{ compiled_code }}

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

dbt = dbtObj(spark.table)

# Generate incremental DataFrame
df = model(dbt, spark)

# Write temp data as a table in the Hive metastore
tmp_table = "{{ tmp_relation }}"
location_root = "{{ config.get('location_root', '') }}"
tmp_location = location_root.rstrip("/") + "/" + tmp_table

print(f"Creating temp table: {tmp_table}")
df.write.mode("overwrite").format(file_format).option("path", tmp_location).saveAsTable(tmp_table)

# Now use SQL to merge temp table into target
merge_sql = f"""
MERGE INTO {{ target_relation }} AS target
USING {tmp_table} AS source
ON {" AND ".join([f"target.{key} = source.{key}" for key in {{ unique_key }}])}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

print("Running merge SQL:")
print(merge_sql)

spark.sql(merge_sql)

# Clean up
print(f"Dropping temp table: {tmp_table}")
spark.sql(f"DROP TABLE IF EXISTS {tmp_table} PURGE")
{% endmacro %}

# A table model gives every adapter a committed table to write into, without
# depending on run-operation itself to create it. Redshift sizes the column
# from this literal, so it must be longer than every tag the tests insert.
txn_probe_sql = """
{{ config(materialized='table') }}
select cast('initial_row_sized_for_every_tag' as {{ dbt.type_string() }}) as probe_tag
"""

transaction_probe_macros_sql = """
{% macro txn_probe_relation() %}
  {{ return(api.Relation.create(
      database=target.database, schema=target.schema, identifier='txn_probe'
  )) }}
{% endmacro %}

{% macro probe_insert_statement(tag) %}
  {% call statement('probe_insert') %}
    insert into {{ txn_probe_relation() }} values ('{{ tag }}')
  {% endcall %}
{% endmacro %}

{% macro probe_insert_run_query(tag) %}
  {% do run_query("insert into " ~ txn_probe_relation() ~ " values ('" ~ tag ~ "')") %}
{% endmacro %}

{% macro probe_insert_then_fail(tag) %}
  {% call statement('probe_insert') %}
    insert into {{ txn_probe_relation() }} values ('{{ tag }}')
  {% endcall %}
  {% do exceptions.raise_compiler_error("intentional failure after insert") %}
{% endmacro %}
"""

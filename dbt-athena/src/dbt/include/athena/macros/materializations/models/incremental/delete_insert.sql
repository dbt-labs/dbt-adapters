{#-
  delete+insert for Iceberg tables.

  Same semantics as dbt's default__get_delete_insert_merge_sql: every target row whose
  unique_key appears in the new data (and that satisfies incremental_predicates, which are
  evaluated against the target) is deleted, then every new row is inserted. unique_key may
  be a single column or a list, and does not need to be unique per row: it names what a run
  replaces wholesale (for example every row of a day or of a customer).

  Athena has no multi-statement transactions, so the default DELETE followed by INSERT
  would be two Iceberg commits, with a window in which readers see the replaced keys
  missing. Instead both happen in one MERGE, i.e. one commit. The source of the MERGE is the
  new rows plus, for each distinct key, one extra row flagged for deletion; only flagged
  rows can match, so they delete every target row of their key and the new rows are
  inserted. Source columns are prefixed so that unqualified incremental_predicates resolve
  to the target, as they do in the default implementation.
-#}

{% macro get_iceberg_delete_insert_merge_sql(target_relation, tmp_relation, unique_key_cols, dest_columns, incremental_predicates) %}
  {%- set src_prefix = 'dbt_src__' -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
  {%- set src_select = [] -%}
  {%- set src_values = [] -%}
  {%- for col in dest_columns -%}
    {%- do src_select.append(col.quoted ~ ' as "' ~ src_prefix ~ col.name | lower ~ '"') -%}
    {%- do src_values.append('DBT_INTERNAL_SOURCE."' ~ src_prefix ~ col.name | lower ~ '"') -%}
  {%- endfor -%}
  {%- set src_select_csv = src_select | join(', ') -%}
  {%- set key_csv = unique_key_cols | join(', ') -%}
  merge into {{ target_relation }} as DBT_INTERNAL_DEST
  using (
      select {{ src_select_csv }}, false as "{{ src_prefix }}delete" from {{ tmp_relation }}
      union all
      select {{ src_select_csv }}, true as "{{ src_prefix }}delete" from (
          select *, row_number() over (partition by {{ key_csv }}) as "{{ src_prefix }}rn"
          from {{ tmp_relation }}
      ) as dbt_one_row_per_key
      where "{{ src_prefix }}rn" = 1
  ) as DBT_INTERNAL_SOURCE
  on (
      {%- for key in unique_key_cols %}
      DBT_INTERNAL_DEST.{{ key }} = DBT_INTERNAL_SOURCE."{{ src_prefix }}{{ key | lower }}" and
      {%- endfor %}
      DBT_INTERNAL_SOURCE."{{ src_prefix }}delete"
  )
  when matched {%- if incremental_predicates %} and ({{ incremental_predicates | join(') and (') }}) {%- endif %} then delete
  when not matched and not DBT_INTERNAL_SOURCE."{{ src_prefix }}delete" then
      insert ({{ dest_cols_csv }}) values ({{ src_values | join(', ') }})
{% endmacro %}


{% macro iceberg_delete_insert_batched(tmp_relation, target_relation, unique_key_cols, dest_columns, incremental_predicates) %}
  {#- Fallback when one MERGE would write more than Athena's partition limit, or when
      force_batch is set: a DELETE, then the existing per-partition batched INSERT.
      Several commits, so readers can observe the replaced keys missing in between.
      Athena's DELETE takes no table alias: predicates written against DBT_INTERNAL_DEST
      only work in the single-MERGE path, so they are rejected here with a clear error. -#}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
  {%- set key_csv = unique_key_cols | join(', ') -%}
  {% do log('delete+insert on ' ~ target_relation ~ ' is running in batches: it is not atomic.', info=True) %}
  {%- for predicate in incremental_predicates or [] -%}
    {%- if 'dbt_internal_dest.' in predicate | lower -%}
      {% do exceptions.raise_compiler_error("delete+insert fell back to batched mode (force_batch, or more partitions than Athena allows in one statement), where incremental_predicates cannot reference DBT_INTERNAL_DEST. Write the predicate with unqualified target columns: " ~ predicate) %}
    {%- endif -%}
  {%- endfor -%}
  {%- set delete_sql -%}
      delete from {{ target_relation }}
      where ({{ key_csv }}) in (select distinct {{ key_csv }} from {{ tmp_relation }})
      {%- for predicate in incremental_predicates or [] %}
        and ({{ predicate }})
      {%- endfor %}
  {%- endset -%}
  {%- do run_query(delete_sql) -%}
  {% do batch_incremental_insert(tmp_relation, target_relation, dest_cols_csv) %}
{% endmacro %}


{% macro iceberg_delete_insert(
    on_schema_change,
    tmp_relation,
    target_relation,
    unique_key,
    incremental_predicates,
    existing_relation,
    force_batch,
    statement_name="main"
  )
%}
    {%- if not unique_key -%}
      {#- Like dbt's default delete+insert: without a unique_key (none, '' or []) there is nothing to delete. -#}
      {{ return(incremental_insert(on_schema_change, tmp_relation, target_relation, existing_relation, force_batch, statement_name)) }}
    {%- endif -%}

    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {% endif %}
    {%- if unique_key is sequence and unique_key is not string -%}
      {%- set unique_key_cols = unique_key -%}
    {%- else -%}
      {%- set unique_key_cols = [unique_key] -%}
    {%- endif -%}

    {%- if force_batch -%}
      {% do iceberg_delete_insert_batched(tmp_relation, target_relation, unique_key_cols, dest_columns, incremental_predicates) %}
      {%- set query_result = 'BATCHED' -%}
    {%- else -%}
      {%- set merge_sql = get_iceberg_delete_insert_merge_sql(target_relation, tmp_relation, unique_key_cols, dest_columns, incremental_predicates) -%}
      {%- set query_result = adapter.run_query_with_partitions_limit_catching(merge_sql) -%}
      {%- do log('QUERY RESULT: ' ~ query_result) -%}
      {%- if query_result == 'TOO_MANY_OPEN_PARTITIONS' -%}
        {% do iceberg_delete_insert_batched(tmp_relation, target_relation, unique_key_cols, dest_columns, incremental_predicates) %}
      {%- endif -%}
    {%- endif -%}

    SELECT '{{ query_result }}'
{%- endmacro %}

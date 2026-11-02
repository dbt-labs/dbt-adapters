{#-
  The global seed materialization, with v2's SQL: the table is truncated, or dropped and recreated
  in one request, then loaded with literal `insert ... values` batches. The default load needs
  parameter bindings, which the driver lacks.

  The service has no transactions, so a load that fails part-way leaves the table empty or
  partly loaded until the seed is re-run.
-#}

{#- drop and create in one request; also replaces a table the listing typed as external -#}
{% macro lakecompute__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
  {%- set columns = [] -%}
  {%- for col_name in agate_table.column_names -%}
    {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
    {%- set type = column_override.get(col_name, inferred_type) -%}
    {%- do columns.append(adapter.quote_seed_column(col_name | string, quote_seed_column) ~ ' ' ~ type) -%}
  {%- endfor -%}

  {%- set sql -%}
    drop table if exists {{ this.render() }};
create table {{ this.render() }} ({{ columns | join(', ') }})
  {%- endset -%}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}


{#- a full refresh skips the default's separate drop: create_csv_table drops in the same request -#}
{% macro lakecompute__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
  {% if full_refresh %}
    {{ return(create_csv_table(model, agate_table)) }}
  {% endif %}
  {{ adapter.truncate_relation(old_relation) }}
  {{ return("truncate table " ~ old_relation.render()) }}
{% endmacro %}


{% macro lakecompute__load_csv_rows(model, agate_table) %}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
  {{ return(adapter.load_seed_rows(this, agate_table, quote_seed_column)) }}
{% endmacro %}

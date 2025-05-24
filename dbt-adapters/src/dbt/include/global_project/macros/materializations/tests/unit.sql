{% macro get_create_table_as_sql_with_sql_header(temporary, relation, sql, sql_header) -%}
  {{ sql_header }}
  {{ adapter.dispatch('get_create_table_as_sql', 'dbt')(temporary, relation, sql) }}
{%- endmacro %}
{% macro get_unit_test_sql_with_sql_header(main_sql, expected_fixture_sql, expected_column_names, sql_header) -%}
  {{ sql_header }}
  {{ adapter.dispatch('get_unit_test_sql', 'dbt')(main_sql, expected_fixture_sql, expected_column_names) }}
{%- endmacro %}

{%- materialization unit, default -%}

  {% set relations = [] %}

  {% set expected_rows = config.get('expected_rows') %}
  {% set expected_sql = config.get('expected_sql') %}
  {%- set sql_header = tested_node_config.get("sql_header", "") if tested_node_config is defined else "" -%}
  {% set tested_expected_column_names = expected_rows[0].keys() if (expected_rows | length ) > 0 else get_columns_in_query(sql, sql_header) %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}
  {% do run_query(get_create_table_as_sql_with_sql_header(True, temp_relation, get_empty_subquery_sql(sql), sql_header)) %}
  {%- set columns_in_relation = adapter.get_columns_in_relation(temp_relation) -%}
  {%- set column_name_to_data_types = {} -%}
  {%- for column in columns_in_relation -%}
  {%-   do column_name_to_data_types.update({column.name|lower: column.data_type}) -%}
  {%- endfor -%}

  {% if not expected_sql %}
  {%   set expected_sql = get_expected_sql(expected_rows, column_name_to_data_types) %}
  {% endif %}
  {% set unit_test_sql = get_unit_test_sql_with_sql_header(sql, expected_sql, tested_expected_column_names, sql_header) %}

  {% call statement('main', fetch_result=True) -%}

    {{ unit_test_sql }}

  {%- endcall %}

  {% do adapter.drop_relation(temp_relation) %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}

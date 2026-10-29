{#- information_schema.columns is unusable on the service (every column_name is '__') -#}
{% macro lakecompute__get_columns_in_relation(relation) -%}
  {% set get_columns_sql %}
    select
        column_name,
        column_type                as data_type,
        null::integer              as character_maximum_length,
        null::integer              as numeric_precision,
        null::integer              as numeric_scale
    from (describe {{ relation.render() }})
  {% endset %}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    {{ get_columns_sql }}
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro lakecompute__drop_table(relation) -%}
  {#- no `cascade`: unsupported for Iceberg -#}
  drop table if exists {{ relation.render() }}
{%- endmacro %}


{% macro lakecompute__drop_view(relation) -%}
  drop view if exists {{ relation.render() }}
{%- endmacro %}


{% macro bigquery__create_csv_table(model, agate_table) %}
  {# When load_csv_rows runs, BigQuery's load job creates the table implicitly.
     When the table has no rows (e.g. --empty), load_csv_rows is skipped,
     so we must create the table explicitly via DDL. #}
  {% if (agate_table.rows | length) == 0 %}
    {{ return(default__create_csv_table(model, agate_table)) }}
  {% endif %}
{% endmacro %}

{% macro bigquery__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {{ adapter.drop_relation(old_relation) }}
    {# When --empty is used, load_csv_rows is skipped so the table won't be
       recreated implicitly by load_dataframe. Create it explicitly via DDL. #}
    {% if (agate_table.rows | length) == 0 %}
        {{ return(default__create_csv_table(model, agate_table)) }}
    {% endif %}
{% endmacro %}

{% macro bigquery__load_csv_rows(model, agate_table) %}

  {%- set column_override = model['config'].get('column_types', {}) -%}
  {{ adapter.load_dataframe(model['database'], model['schema'], model['alias'],
  							agate_table, column_override, model['config']['delimiter']) }}

  {% call statement() %}
    alter table {{ this.render() }} set {{ bigquery_table_options(config, model) }}
  {% endcall %}

  {% if config.persist_relation_docs() and 'description' in model %}

  	{{ adapter.update_table_description(model['database'], model['schema'], model['alias'], model['description']) }}
  {% endif %}
{% endmacro %}

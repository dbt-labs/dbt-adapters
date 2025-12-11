
{% macro bigquery__create_csv_table(model, agate_table) %}
    -- no-op
{% endmacro %}

{% macro bigquery__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {{ adapter.drop_relation(old_relation) }}
{% endmacro %}

{% macro bigquery__load_csv_rows(model, agate_table) %}

  {%- set column_override = model['config'].get('column_types', {}) -%}

  {%- set size = None -%}
  {% if flags.EMPTY %}
    {% set size = 0 %}
    {% set agate_table = load_agate_table(row_limit=1) %}
  {% endif %}

  {{ adapter.load_dataframe(model['database'], model['schema'], model['alias'],
  							agate_table, column_override, model['config']['delimiter'], size) }}

  {% call statement() %}
    alter table {{ this.render() }} set {{ bigquery_table_options(config, model) }}
  {% endcall %}

  {% if config.persist_relation_docs() and 'description' in model %}

  	{{ adapter.update_table_description(model['database'], model['schema'], model['alias'], model['description']) }}
  {% endif %}
{% endmacro %}

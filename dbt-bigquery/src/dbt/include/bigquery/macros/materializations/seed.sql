
{% macro bigquery__create_csv_table(model, agate_table, relation=none) %}
    -- no-op
{% endmacro %}

{% macro bigquery__reset_csv_table(model, full_refresh, old_relation, agate_table, relation=none) %}
    {{ adapter.drop_relation(old_relation) }}
{% endmacro %}

{% macro bigquery__load_csv_rows(model, agate_table, relation=none) %}
  {%- set relation = relation if relation is not none else this -%}

  {%- set column_override = model['config'].get('column_types', {}) -%}
  {{ adapter.load_dataframe(relation.database, relation.schema, relation.identifier,
  							agate_table, column_override, model['config']['delimiter']) }}

  {% call statement() %}
    alter table {{ relation.render() }} set {{ bigquery_table_options(config, model) }}
  {% endcall %}

  {% if config.persist_relation_docs() and 'description' in model %}

  	{{ adapter.update_table_description(relation.database, relation.schema, relation.identifier, model['description']) }}
  {% endif %}
{% endmacro %}

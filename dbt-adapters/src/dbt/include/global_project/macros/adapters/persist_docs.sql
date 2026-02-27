{% macro alter_column_comment(relation, column_dict) -%}
  {{ return(adapter.dispatch('alter_column_comment', 'dbt')(relation, column_dict)) }}
{% endmacro %}

{% macro default__alter_column_comment(relation, column_dict) -%}
  {{ exceptions.raise_not_implemented(
    'alter_column_comment macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}


{% macro alter_relation_comment(relation, relation_comment) -%}
  {{ return(adapter.dispatch('alter_relation_comment', 'dbt')(relation, relation_comment)) }}
{% endmacro %}

{% macro default__alter_relation_comment(relation, relation_comment) -%}
  {{ exceptions.raise_not_implemented(
    'alter_relation_comment macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}


{% macro persist_docs(relation, model, for_relation=true, for_columns=true) -%}
  {{ return(adapter.dispatch('persist_docs', 'dbt')(relation, model, for_relation, for_columns)) }}
{% endmacro %}

{#-- Warns when the YAML schema defines columns that don't exist in the database relation.
     Returns the list of missing column names. Dispatched so adapters with nested columns
     (e.g. BigQuery STRUCTs) can provide custom comparison logic. --#}
{% macro warn_for_missing_doc_columns(relation, column_dict) %}
  {{ return(adapter.dispatch('warn_for_missing_doc_columns', 'dbt')(relation, column_dict)) }}
{% endmacro %}

{% macro default__warn_for_missing_doc_columns(relation, column_dict) %}
  {#-- Get the column names that actually exist in the database --#}
  {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
  {% set existing_columns_lower = existing_columns | map("lower") | list %}

  {#-- Find documented columns that aren't in the relation --#}
  {% set missing = [] %}
  {% for col_name in column_dict %}
    {% if col_name | lower not in existing_columns_lower %}
      {% do missing.append(col_name) %}
    {% endif %}
  {% endfor %}

  {% if missing | length > 0 %}
    {{ exceptions.warn("In relation " ~ relation.render() ~ ": The following columns are specified in the schema but are not present in the database: " ~ missing | join(", ")) }}
  {% endif %}
  {{ return(missing) }}
{% endmacro %}

{% macro default__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_relation and config.persist_relation_docs() and model.description %}
    {% do run_query(alter_relation_comment(relation, model.description)) %}
  {% endif %}

  {% if for_columns and config.persist_column_docs() and model.columns %}
    {% do warn_for_missing_doc_columns(relation, model.columns) %}
    {#-- Guard against empty SQL: alter_column_comment may return an empty string
         when no documented columns match actual columns (e.g. Postgres filters by
         existing columns). Running an empty query would cause a database error. --#}
    {% set alter_comment_sql = alter_column_comment(relation, model.columns) %}
    {% if alter_comment_sql and alter_comment_sql | trim | length > 0 %}
      {% do run_query(alter_comment_sql) %}
    {% endif %}
  {% endif %}
{% endmacro %}

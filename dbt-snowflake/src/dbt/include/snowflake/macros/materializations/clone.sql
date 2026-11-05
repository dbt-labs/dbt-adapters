{% macro snowflake__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}

{% macro snowflake__get_clone_target_relation(this_relation, source_relation) %}
    {{ return(source_relation.incorporate(
        path={'database': this_relation.database, 'schema': this_relation.schema, 'identifier': this_relation.identifier},
        type='table'
    ).quote(
        database=this_relation.quote_policy.database,
        schema=this_relation.quote_policy.schema,
        identifier=this_relation.quote_policy.identifier
    )) }}
{% endmacro %}

{% macro snowflake__clone_target_drop_reason(existing_relation, target_relation) %}
    {% if not existing_relation.is_table %}
        {{ return("it is of type " ~ existing_relation.type) }}
    {% elif existing_relation.is_iceberg_format != target_relation.is_iceberg_format %}
        {{ return("it is " ~ ("an Iceberg table" if existing_relation.is_iceberg_format else "a regular table")
            ~ " and the clone source is " ~ ("an Iceberg table" if target_relation.is_iceberg_format else "a regular table")) }}
    {% elif target_relation.needs_to_drop(existing_relation) %}
        {{ return("it cannot be replaced in place by the clone") }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}

{% macro snowflake__create_or_replace_clone(this_relation, defer_relation) %}
    create or replace
      {% if this_relation.is_iceberg_format -%}
        iceberg
      {%- else -%}
        {{ "transient" if config.get("transient", true) }}
      {%- endif %}
      table {{ this_relation }}
      clone {{ defer_relation }}
      {{ snowflake__copy_grants_clause() }}
      {{ "copy tags" if config.get("copy_tags", false) }}
{% endmacro %}

{# Mirrors the default clone materialization, using the snowflake__ clone target helpers above. #}
{%- materialization clone, adapter='snowflake' -%}

  {%- set relations = {'relations': []} -%}

  {%- if not defer_relation -%}
      -- nothing to do
      {{ log("No relation found in state manifest for " ~ model.unique_id, info=True) }}
      {{ return(relations) }}
  {%- endif -%}

  {%- set existing_relation = load_cached_relation(this) -%}

  {%- if existing_relation and not flags.FULL_REFRESH -%}
      -- noop!
      {{ log("Relation " ~ existing_relation ~ " already exists", info=True) }}
      {{ return(relations) }}
  {%- endif -%}

  {%- set other_existing_relation = load_cached_relation(defer_relation) -%}

  {% set can_clone_table = can_clone_table() %}
  {%- set grant_config = config.get('grants') -%}

  {%- if other_existing_relation and other_existing_relation.type == 'table' and can_clone_table -%}

      {%- set target_relation = snowflake__get_clone_target_relation(this, other_existing_relation) -%}
      {%- set drop_reason = snowflake__clone_target_drop_reason(existing_relation, target_relation) if existing_relation is not none else none -%}
      {% if drop_reason %}
        {{ log("Dropping relation " ~ existing_relation.render() ~ " because " ~ drop_reason) }}
        {{ drop_relation_if_exists(existing_relation) }}
      {% endif %}

      {% if target_relation.database == defer_relation.database and
            target_relation.schema == defer_relation.schema and
            target_relation.identifier == defer_relation.identifier %}
        {{ log("Target relation and defer relation are the same, skipping clone for relation: " ~ target_relation.render()) }}
      {% else %}
        {% call statement('main') %}
            {{ create_or_replace_clone(target_relation, defer_relation) }}
        {% endcall %}
      {% endif %}
      {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
      {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
      {% do persist_docs(target_relation, model) %}

      {{ return({'relations': [target_relation]}) }}

  {%- else -%}

      {%- set target_relation = this.incorporate(type='view') -%}

      -- reuse the view materialization
      -- TODO: support actual dispatch for materialization macros
      -- Tracking ticket: https://github.com/dbt-labs/dbt-core/issues/7799
      {% set search_name = "materialization_view_" ~ adapter.type() %}
      {% if not search_name in context %}
          {% set search_name = "materialization_view_default" %}
      {% endif %}
      {% set materialization_macro = context[search_name] %}
      {% set relations = materialization_macro() %}
      {{ return(relations) }}

  {%- endif -%}

{%- endmaterialization -%}

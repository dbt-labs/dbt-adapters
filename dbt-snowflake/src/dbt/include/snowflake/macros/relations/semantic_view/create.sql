{% macro snowflake__get_create_semantic_view_sql(relation, sql) -%}
{#-
--  Produce DDL that creates a semantic view
--
--  Args:
--  - relation: Union[SnowflakeRelation, str]
--      - SnowflakeRelation - required for relation.render()
--      - str - is already the rendered relation name
--  - sql: str - the code defining the model
--  Returns:
--      A valid DDL statement which will result in a new semantic view.
-#}

  create or replace semantic view {{ relation }}
  {{ sql }}

{%- endmacro %}


{% macro snowflake__create_or_replace_semantic_view() %}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_semantic_view) -%}
  {%- set copy_grants = config.get('copy_grants', default=false) -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema, database=database,
      type='semantic_view') -%}
  {% set grant_config = config.get('grants') %}

  {%- if copy_grants -%}
    {#- Normalize SQL and append COPY GRANTS if not already present (case-insensitive) -#}
    {%- set _sql_norm = sql | trim -%}
    {%- set _sql_norm = _sql_norm[:-1] if _sql_norm[-1:] == ';' else _sql_norm -%}
    {%- set _sql_norm = _sql_norm | trim -%}
    {%- set _ends = (_sql_norm | lower)[-11:] -%}
    {%- if _ends != 'copy grants' -%}
      {%- set sql = sql ~ '\nCOPY GRANTS' -%}
    {%- endif -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks) }}

  -- build model
  {% call statement('main') -%}
    {{ snowflake__get_create_semantic_view_sql(target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmacro %}

{% materialization table, adapter='redshift' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}

  {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}
  {%- set is_iceberg = (catalog_relation is not none and catalog_relation.table_format == 'iceberg') -%}

  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if is_iceberg %}
    -- Redshift Iceberg has no CREATE OR REPLACE and can't rename. DROP TABLE only
    -- removes the Glue catalog entry (S3 data is left behind), and CTAS requires an
    -- empty LOCATION, so we: drop the catalog entry (if any), purge the S3 prefix,
    -- then CTAS.
    {% if existing_relation is not none %}
      {% call statement('drop_iceberg_target') -%}
        drop table if exists {{ target_relation }}
      {%- endcall %}
    {% endif %}
    {% do adapter.delete_from_s3(catalog_relation.location) %}
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, target_relation, sql) }}
    {%- endcall %}
  {% else %}
    -- build model
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {%- endcall %}

    -- cleanup
    {% if existing_relation is not none %}
       /* Do the equivalent of rename_if_exists. 'existing_relation' could have been dropped
          since the variable was first set. */
      {% set existing_relation = load_cached_relation(existing_relation) %}
      {% if existing_relation is not none %}
          {% if existing_relation.can_be_renamed %}
              {{ adapter.rename_relation(existing_relation, backup_relation) }}
          {% else  %}
              {{ drop_relation_if_exists(existing_relation) }}
          {% endif %}
      {% endif %}
    {% endif %}


    {{ adapter.rename_relation(intermediate_relation, target_relation) }}

    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {# Iceberg external tables don't support column comments / persist_docs #}
  {% if not is_iceberg %}
    {% do persist_docs(target_relation, model) %}
  {% endif %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {% if not is_iceberg %}
    {{ drop_relation_if_exists(backup_relation) }}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

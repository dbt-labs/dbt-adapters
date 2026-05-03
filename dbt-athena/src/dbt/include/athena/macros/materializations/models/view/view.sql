{% materialization view, adapter='athena' -%}
    {%- if adapter.is_s3_table_bucket(database) -%}
      {% do exceptions.raise_compiler_error("CREATE VIEW is not supported on S3 Table Bucket catalogs by AWS.") %}
    {%- endif -%}
    {%- set identifier = model['alias'] -%}
    {%- set versions_to_keep = config.get('versions_to_keep', default=4) -%}
    {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='view') -%}

    {% set to_return = create_or_replace_view(run_outside_transaction_hooks=False) %}

    {% do adapter.expire_glue_table_versions(target_relation, versions_to_keep, False) %}

    {% set target_relation = this.incorporate(type='view') %}
    {% do persist_docs(target_relation, model) %}

    {% do return(to_return) %}
{%- endmaterialization %}

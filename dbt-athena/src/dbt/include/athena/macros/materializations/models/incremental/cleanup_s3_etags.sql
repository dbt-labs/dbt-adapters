{% macro cleanup_s3_etags(initial_etags_dict, incremental_source_name, incremental_source_table_name) %}
  {#
    Deletes S3 files with unchanged ETags after incremental model run.

    Args:
      initial_etags_dict: Dictionary of ETags captured before model run
      incremental_source_name: Source name from config (e.g., 'increment_significant')
      incremental_source_table_name: Source table name from config (e.g., 'applicants')

    Requires:
      dbt_project.yml variable 's3_location_prefix_to_validate' must be defined

    Returns:
      Dictionary with cleanup results (aligned with AWS delete_objects response):
      {
        'deleted': Number of files successfully deleted,
        'skipped': Number of files skipped
      }
  #}

  {% set cleanup_result = {'deleted': 0, 'skipped': 0} %}

  {# Only proceed if source config is provided #}
  {% if incremental_source_name and incremental_source_table_name %}
    {# Resolve source to Relation object #}
    {% set source_relation = source(incremental_source_name, incremental_source_table_name) %}

    {# Get REQUIRED safety prefix from dbt project variable (no default) #}
    {% set safety_prefix = var('s3_location_prefix_to_validate') %}

    {# Call adapter method with safety prefix #}
    {% set cleanup_result = adapter.delete_unchanged_s3_files(source_relation, initial_etags_dict, safety_prefix) %}
  {% endif %}

  {{ return(cleanup_result) }}
{% endmacro %}

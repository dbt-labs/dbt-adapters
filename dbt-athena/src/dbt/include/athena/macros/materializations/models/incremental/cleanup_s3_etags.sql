{% macro cleanup_s3_etags(initial_etags_dict, incremental_source_name, incremental_source_table_name) %}
  {#
    Deletes S3 files with unchanged ETags after incremental model run.
    
    Args:
      initial_etags_dict: Dictionary of ETags captured before model run
      incremental_source_name: Source name from config (e.g., 'increment_significant')
      incremental_source_table_name: Source table name from config (e.g., 'applicants')
    
    Returns:
      Dictionary with cleanup results:
      {
        'deleted': Number of files deleted,
        'skipped': Number of files skipped,
        'errors': Number of errors
      }
  #}
  
  {% set cleanup_result = {'deleted': 0, 'skipped': 0, 'errors': 0} %}
  
  {# Only proceed if source config is provided #}
  {% if incremental_source_name and incremental_source_table_name %}
    {# Resolve source to Relation object #}
    {% set source_relation = source(incremental_source_name, incremental_source_table_name) %}
    
    {# Call adapter method to perform deletion (it handles empty ETags) #}
    {% set cleanup_result = adapter.delete_unchanged_s3_files(source_relation, initial_etags_dict) %}
  {% endif %}
  
  {{ return(cleanup_result) }}
{% endmacro %}



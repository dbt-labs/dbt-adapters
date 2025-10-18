{% macro cleanup_s3_etags(initial_etags_dict, incremental_source_name, incremental_source_table_name) %}
  {#
    Deletes S3 files with unchanged ETags after incremental model run.
    
    Args:
      initial_etags_dict: Dictionary of ETags captured before model run
      incremental_source_name: Source name from config (e.g., 'increment_significant')
      incremental_source_table_name: Source table name from config (e.g., 'applicants')
    
    Returns:
      Number of files deleted (int)
  #}
  
  {% set deleted_count = 0 %}
  
  {# Only proceed if we have ETags and source config #}
  {% if initial_etags_dict and incremental_source_name and incremental_source_table_name %}
    {# Resolve source to Relation object #}
    {% set source_relation = source(incremental_source_name, incremental_source_table_name) %}
    
    {# Call adapter method to perform deletion #}
    {% set deleted_count = adapter.delete_unchanged_s3_files(source_relation, initial_etags_dict) %}
  {% endif %}
  
  {{ return(deleted_count) }}
{% endmacro %}



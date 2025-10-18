{% macro capture_s3_etags(incremental_source_name, incremental_source_table_name) %}
  {#
    Captures S3 file ETags before incremental model run.
    
    Args:
      incremental_source_name: Source name from config (e.g., 'increment_significant')
      incremental_source_table_name: Source table name from config (e.g., 'applicants')
    
    Returns:
      Dictionary of {file_key: etag}
  #}
  
  {# Validate parameters #}
  {% if not incremental_source_name or not incremental_source_table_name %}
    {{ exceptions.raise_compiler_error(
      "Please provide 'incremental_source_name' and 'incremental_source_table_name' in config for incremental strategy"
    ) }}
  {% endif %}
  
  {# Resolve source to Relation object #}
  {% set source_relation = source(incremental_source_name, incremental_source_table_name) %}
  
  {# Capture ETags via adapter method #}
  {% set etags_dict = adapter.get_s3_file_etags(source_relation) %}
  
  {{ return(etags_dict) }}
{% endmacro %}



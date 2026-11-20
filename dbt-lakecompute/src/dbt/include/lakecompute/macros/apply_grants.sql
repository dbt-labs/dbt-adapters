{#- grants are ignored, as in v2: the service has no GRANT -#}
{% macro lakecompute__apply_grants(relation, grant_config, should_revoke=True) %}
{% endmacro %}


{% macro lakecompute__copy_grants() %}
  {{ return(False) }}
{% endmacro %}

{% macro get_submission_method() %}
  {%- if config.get('submission_method', none) is not none -%}
    {{ config.get('submission_method') }}
  {% elif adapter.config.credentials is defined and adapter.config.credentials.submission_method is defined %}
    {{ return(adapter.config.credentials.submission_method) }}
  {% else %}
    {{ return('job_cluster') }}
  {% endif %}
{% endmacro %}

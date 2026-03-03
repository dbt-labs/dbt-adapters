{% macro redshift__use_show_apis() %}
    {{ return(adapter.behavior.redshift_use_show_apis.no_warn) }}
{% endmacro %}

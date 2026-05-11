{% macro redshift__use_show_apis() %}
    {{ return(adapter.use_show_apis()) }}
{% endmacro %}

{% macro redshift__use_grants_extended() %}
    {{ return(adapter.use_grants_extended()) }}
{% endmacro %}

{% macro redshift__drop_without_cascade() %}
    {{ return(adapter.drop_without_cascade()) }}
{% endmacro %}

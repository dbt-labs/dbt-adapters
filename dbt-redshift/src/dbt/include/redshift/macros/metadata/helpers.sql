{% macro redshift__use_show_apis() %}
    {{ return(adapter.use_show_apis()) }}
{% endmacro %}

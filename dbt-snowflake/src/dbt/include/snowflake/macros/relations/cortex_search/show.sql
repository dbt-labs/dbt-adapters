{% macro snowflake__show_cortex_search_service(relation) %}
    {% set query %}
        show cortex search services like '{{ relation.identifier | replace("'", "''") }}'
        in schema {{ relation.include(identifier=False) }}
    {% endset %}
    {{ return(run_query(query)) }}
{% endmacro %}

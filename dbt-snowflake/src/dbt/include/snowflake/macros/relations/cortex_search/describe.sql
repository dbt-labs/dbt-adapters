{% macro snowflake__describe_cortex_search_service(relation) %}
    {% set query %}
        describe cortex search service {{ relation.render() }}
    {% endset %}
    {{ return(run_query(query)) }}
{% endmacro %}

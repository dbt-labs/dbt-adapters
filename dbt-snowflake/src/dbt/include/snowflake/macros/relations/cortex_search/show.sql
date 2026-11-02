{% macro snowflake__get_cortex_search_service(relation) %}
    {% set relation = relation.as_case_sensitive() %}
    {% if not adapter.check_schema_exists(relation.database, relation.schema) %}
        {{ return(none) }}
    {% endif %}
    {% set query %}
        show cortex search services like '{{ relation.identifier | replace("'", "''") }}'
        in schema {{ relation.include(identifier=False) }}
    {% endset %}
    {% set services = run_query(query) %}
    {% set services = services.rename(column_names=services.column_names | map('lower') | list) %}
    {% for service in services %}
        {% if service['name'] == relation.identifier %}
            {{ return(relation.incorporate(type=relation.CortexSearchService)) }}
        {% endif %}
    {% endfor %}
    {{ return(none) }}
{% endmacro %}

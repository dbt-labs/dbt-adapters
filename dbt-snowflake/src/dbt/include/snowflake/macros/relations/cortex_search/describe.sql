{% macro snowflake__describe_cortex_search_service(relation) %}
    {% set query %}
        describe cortex search service {{ relation.render() }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if results.rows | length != 1 %}
        {% do exceptions.raise_compiler_error('Could not read Cortex Search service configuration: ' ~ relation.render()) %}
    {% endif %}
    {% set metadata = {} %}
    {% for name in results.column_names %}
        {% do metadata.update({(name | lower): results.rows[0][name]}) %}
    {% endfor %}
    {% if metadata.get('indexing_error') %}
        {% do exceptions.raise_compiler_error(
            'Cortex Search service ' ~ relation.render() ~ ' reports an indexing error: ' ~
            metadata['indexing_error'] ~ ' Check change tracking on upstream objects. ' ~
            'After replacing an upstream object, re-enable change tracking and run the service with --full-refresh.'
        ) %}
    {% endif %}
    {{ return(metadata) }}
{% endmacro %}

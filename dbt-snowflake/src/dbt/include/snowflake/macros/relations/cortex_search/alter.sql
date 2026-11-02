{% macro snowflake__get_alter_cortex_search_service_sql(relation, changes) %}
    alter cortex search service {{ relation.render() }} set
    {% for name, value in changes.items() %}
        {{ name }} =
        {% if name in ['target_lag', 'comment'] %}
            {{ dbt.string_literal(dbt.escape_single_quotes(value | replace('\\', '\\\\'))) }}
        {% elif name == 'request_logging' %}
            {{ 'TRUE' if value else 'FALSE' }}
        {% else %}
            {{ value }}
        {% endif %}
    {% endfor %}
{% endmacro %}

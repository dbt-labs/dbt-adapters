{% macro snowflake__get_create_cortex_search_service_sql(relation, sql, full_refresh_mode=False) %}
    {% set search_column = config.get('search_column') %}
    {% set attributes = config.get('attributes') %}
    {% set warehouse = config.get('snowflake_warehouse') or target.warehouse %}
    {% set target_lag = config.get('target_lag') %}

    {% for name, value in [('search_column', search_column), ('snowflake_warehouse', warehouse), ('target_lag', target_lag)] %}
        {% if value is not string or not value | trim %}
            {% do exceptions.raise_compiler_error("cortex_search_service requires a non-empty string for `" ~ name ~ "`.") %}
        {% endif %}
    {% endfor %}
    {% if attributes is none %}
        {% set attributes = [] %}
    {% endif %}
    {% if attributes is not sequence or attributes is string or attributes is mapping %}
        {% do exceptions.raise_compiler_error("cortex_search_service `attributes` must be a list of column names.") %}
    {% endif %}
    {% for attribute in attributes %}
        {% if attribute is not string or not attribute | trim %}
            {% do exceptions.raise_compiler_error("cortex_search_service `attributes` must contain non-empty column names.") %}
        {% endif %}
    {% endfor %}

    {% if full_refresh_mode %}
        create or replace cortex search service {{ relation }}
    {% else %}
        create cortex search service if not exists {{ relation }}
    {% endif %}
    on {{ search_column }}
    {% if attributes %}attributes {{ attributes | join(', ') }}{% endif %}
    warehouse = {{ warehouse }}
    target_lag = '{{ target_lag | replace("'", "''") }}'
    as (
        {{ sql }}
    )
{% endmacro %}

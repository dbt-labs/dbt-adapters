{% macro snowflake__get_create_cortex_search_service_sql(relation, sql, full_refresh_mode=False) %}
    {% set search = relation.from_config(config.model) %}
    {% set warehouse = search.warehouse_parameter(target.warehouse) %}

    {% if full_refresh_mode %}
        create or replace cortex search service {{ relation }}
    {% else %}
        create cortex search service if not exists {{ relation }}
    {% endif %}
    on {{ search.search_column }}
    {% if search.primary_key %}primary key ({{ search.primary_key | join(', ') }}){% endif %}
    attributes {{ search.attributes | join(', ') }}
    warehouse = {{ warehouse }}
    target_lag = '{{ snowflake__cortex_search_string_literal(search.target_lag) }}'
    {% if search.embedding_model is not none %}
        {{ optional('embedding_model', snowflake__cortex_search_string_literal(search.embedding_model), "'") }}
    {% endif %}
    {{ optional('refresh_mode', search.refresh_mode) }}
    {{ optional('initialize', search.initialize) }}
    {{ optional('full_index_build_interval_days', search.full_index_build_interval_days) }}
    {{ optional('request_logging', search.request_logging | string | upper if search.request_logging is not none else none) }}
    {{ optional('auto_suspend', search.auto_suspend) }}
    {% if search.comment is not none %}
        {{ optional('comment', snowflake__cortex_search_string_literal(search.comment), "'") }}
    {% endif %}
    as (
        {{ sql }}
    )
{% endmacro %}


{%- macro snowflake__cortex_search_string_literal(value) -%}
    {{- value | replace('\\', '\\\\') | replace("'", "''") -}}
{%- endmacro -%}

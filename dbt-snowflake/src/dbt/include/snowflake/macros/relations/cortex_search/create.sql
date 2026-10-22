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
    target_lag = {{ dbt.string_literal(dbt.escape_single_quotes(search.target_lag | replace('\\', '\\\\'))) }}
    {% if search.embedding_model is not none %}
        {{ optional('embedding_model', dbt.escape_single_quotes(search.embedding_model | replace('\\', '\\\\')), "'") }}
    {% endif %}
    {{ optional('refresh_mode', search.refresh_mode) }}
    {{ optional('initialize', search.initialize) }}
    {{ optional('full_index_build_interval_days', search.full_index_build_interval_days) }}
    {{ optional('request_logging', search.request_logging | string | upper if search.request_logging is not none else none) }}
    {{ optional('auto_suspend', search.auto_suspend) }}
    {% if search.comment is not none %}
        {{ optional('comment', dbt.escape_single_quotes(search.comment | replace('\\', '\\\\')), "'") }}
    {% endif %}
    as (
        {{ sql }}
    )
{% endmacro %}

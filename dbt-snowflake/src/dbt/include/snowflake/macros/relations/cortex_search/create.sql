{% macro snowflake__get_create_cortex_search_service_sql(relation, sql, full_refresh_mode=False) %}
    {% set search_column = config.get('search_column') %}
    {% set attributes = config.get('attributes') %}
    {% set warehouse = config.get('snowflake_warehouse') or target.warehouse %}
    {% set target_lag = config.get('target_lag') %}
    {% set primary_key = config.get('primary_key') %}
    {% set embedding_model = config.get('embedding_model') %}
    {% set refresh_mode = config.get('refresh_mode') %}
    {% set initialize = config.get('initialize') %}
    {% set full_index_build_interval_days = config.get('full_index_build_interval_days') %}
    {% set request_logging = config.get('request_logging') %}
    {% set auto_suspend = config.get('auto_suspend') %}
    {% set comment = config.get('comment') %}

    {% for name, value in [('search_column', search_column), ('snowflake_warehouse', warehouse), ('target_lag', target_lag)] %}
        {% if value is not string or not value | trim %}
            {% do exceptions.raise_compiler_error("cortex_search_service requires a non-empty string for `" ~ name ~ "`.") %}
        {% endif %}
    {% endfor %}
    {% if not attributes %}
        {% do exceptions.raise_compiler_error("cortex_search_service requires a non-empty list for `attributes`.") %}
    {% endif %}
    {% for name, columns in [('attributes', attributes), ('primary_key', primary_key)] %}
        {% if columns is not none %}
            {% if columns is not sequence or columns is string or columns is mapping %}
                {% do exceptions.raise_compiler_error("cortex_search_service `" ~ name ~ "` must be a list of column names.") %}
            {% endif %}
            {% for column in columns %}
                {% if column is not string or not column | trim %}
                    {% do exceptions.raise_compiler_error("cortex_search_service `" ~ name ~ "` must contain non-empty column names.") %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
    {% if embedding_model is not none and (embedding_model is not string or not embedding_model | trim) %}
        {% do exceptions.raise_compiler_error("cortex_search_service `embedding_model` must be a non-empty string.") %}
    {% endif %}
    {% if comment is not none and comment is not string %}
        {% do exceptions.raise_compiler_error("cortex_search_service `comment` must be a string.") %}
    {% endif %}
    {% for name, value, allowed in [('refresh_mode', refresh_mode, ['FULL', 'INCREMENTAL']), ('initialize', initialize, ['ON_CREATE', 'ON_SCHEDULE'])] %}
        {% if value is not none and (value is not string or value | trim | upper not in allowed) %}
            {% do exceptions.raise_compiler_error("cortex_search_service `" ~ name ~ "` must be one of " ~ allowed | join(', ') ~ ".") %}
        {% endif %}
    {% endfor %}
    {% for name, value, minimum in [('full_index_build_interval_days', full_index_build_interval_days, 1), ('auto_suspend', auto_suspend, 1800)] %}
        {% if value is not none and (value is not integer or value < minimum) %}
            {% do exceptions.raise_compiler_error("cortex_search_service `" ~ name ~ "` must be an integer >= " ~ minimum ~ ".") %}
        {% endif %}
    {% endfor %}
    {% if full_index_build_interval_days is not none and not primary_key %}
        {% do exceptions.raise_compiler_error("cortex_search_service `full_index_build_interval_days` requires a non-empty `primary_key`.") %}
    {% endif %}
    {% if request_logging is not none and request_logging is not boolean %}
        {% do exceptions.raise_compiler_error("cortex_search_service `request_logging` must be a boolean.") %}
    {% endif %}

    {% if full_refresh_mode %}
        create or replace cortex search service {{ relation }}
    {% else %}
        create cortex search service if not exists {{ relation }}
    {% endif %}
    on {{ search_column }}
    {% if primary_key %}primary key ({{ primary_key | join(', ') }}){% endif %}
    attributes {{ attributes | join(', ') }}
    warehouse = {{ warehouse }}
    target_lag = '{{ snowflake__cortex_search_string_literal(target_lag) }}'
    {% if embedding_model is not none %}embedding_model = '{{ snowflake__cortex_search_string_literal(embedding_model) }}'{% endif %}
    {% if refresh_mode is not none %}refresh_mode = {{ refresh_mode | trim | upper }}{% endif %}
    {% if initialize is not none %}initialize = {{ initialize | trim | upper }}{% endif %}
    {% if full_index_build_interval_days is not none %}full_index_build_interval_days = {{ full_index_build_interval_days }}{% endif %}
    {% if request_logging is not none %}request_logging = {{ 'TRUE' if request_logging else 'FALSE' }}{% endif %}
    {% if auto_suspend is not none %}auto_suspend = {{ auto_suspend }}{% endif %}
    {% if comment is not none %}comment = '{{ snowflake__cortex_search_string_literal(comment) }}'{% endif %}
    as (
        {{ sql }}
    )
{% endmacro %}


{%- macro snowflake__cortex_search_string_literal(value) -%}
    {{- value | replace('\\', '\\\\') | replace("'", "''") -}}
{%- endmacro -%}

{% macro scalar_function_sql(target_relation) %}
    {{ return(adapter.dispatch('scalar_function_sql', 'dbt')(target_relation)) }}
{% endmacro %}

{% macro default__scalar_function_sql(target_relation) %}
    {{ scalar_function_create_replace_signature_sql(target_relation) }}
    {{ scalar_function_body_sql() }};
{% endmacro %}

{% macro scalar_function_create_replace_signature_sql(target_relation) %}
    {{ return(adapter.dispatch('scalar_function_create_replace_signature_sql', 'dbt')(target_relation)) }}
{% endmacro %}

{% macro default__scalar_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql()}})
    RETURNS {{ model.returns.data_type }}
    {{ scalar_function_volatility_sql() }}
    AS
{% endmacro %}

{% macro formatted_scalar_function_args_sql() %}
    {{ return(adapter.dispatch('formatted_scalar_function_args_sql', 'dbt')()) }}
{% endmacro %}

{% macro default__formatted_scalar_function_args_sql() %}
    {% set args = [] %}
    {% for arg in model.arguments -%}
        {%- do args.append(arg.name ~ ' ' ~ arg.data_type) -%}
    {%- endfor %}
    {{ args | join(', ') }}
{% endmacro %}

{% macro scalar_function_body_sql() %}
    {{ return(adapter.dispatch('scalar_function_body_sql', 'dbt')()) }}
{% endmacro %}

{% macro default__scalar_function_body_sql() %}
    $$
       {{ model.compiled_code }}
    $$ LANGUAGE SQL
{% endmacro %}

{% macro scalar_function_volatility_sql() %}
    {{ return(adapter.dispatch('scalar_function_volatility_sql', 'dbt')()) }}
{% endmacro %}

{% macro default__scalar_function_volatility_sql() %}
    {% set volatility = model.config.get('volatility') %}
    {% if volatility == 'deterministic' %}
        IMMUTABLE
    {% elif volatility == 'stable' %}
        STABLE
    {% elif volatility == 'non-deterministic' %}
        VOLATILE
    {% elif volatility != none %}
        {# This shouldn't happen unless a new volatility is invented #}
        {% do unsupported_volatility_warning(volatility) %}
    {% endif %}
    {# If no volatility is set, don't add anything and let the data warehouse default it #}
{% endmacro %}

{% macro unsupported_volatility_warning(volatility) %}
    {{ return(adapter.dispatch('unsupported_volatility_warning', 'dbt')(volatility)) }}
{% endmacro %}

{% macro default__unsupported_volatility_warning(volatility) %}
    {% set msg = "Found `" ~ volatility ~ "` volatility specified on function `" ~ model.name ~ "`. This volatility is not supported by " ~ adapter.type() ~ ", and will be ignored" %}
    {% do exceptions.warn(msg) %}
{% endmacro %}

{% macro get_python_runtime_version() %}
    {{ return(adapter.dispatch('get_python_runtime_version', 'dbt')()) }}
{% endmacro %}

{% macro default__get_python_runtime_version() %}
    {% set runtime_version = model.config.get('runtime_version') %}
    {% if runtime_version == none %}
        {% do exceptions.raise_compiler_error("A `runtime_version` is required for python user defined functions") %}
    {% endif %}
    {{ return(runtime_version) }}
{% endmacro %}

{% macro get_python_entry_point() %}
    {{ return(adapter.dispatch('get_python_entry_point', 'dbt')()) }}
{% endmacro %}

{% macro default__get_python_entry_point() %}
    {% set entry_point = model.config.get('entry_point') %}
    {% if entry_point == none %}
        {% do exceptions.raise_compiler_error("An `entry_point` is required for python user defined functions") %}
    {% endif %}
    {{ return(entry_point) }}
{% endmacro %}

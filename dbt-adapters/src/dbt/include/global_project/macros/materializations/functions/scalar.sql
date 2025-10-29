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
    {% if model.config.get('volatility') == 'deterministic' %}
        IMMUTABLE
    {% elif model.config.get('volatility') == 'stable' %}
        STABLE
    {% elif model.config.get('volatility') == 'non-deterministic' %}
        VOLATILE
    {% endif %}
    {# If no volatility is set, don't add anything and let the data warehouse default it #}
{% endmacro %}

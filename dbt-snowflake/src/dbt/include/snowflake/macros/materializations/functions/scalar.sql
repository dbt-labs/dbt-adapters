{% macro snowflake__scalar_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql()}})
    RETURNS {{ model.returns.data_type }}
    LANGUAGE SQL
    {{ scalar_function_volatility_sql() }}
    AS
{% endmacro %}

{% macro snowflake__scalar_function_body_sql() %}
    $$
       {{ model.compiled_code }}
    $$
{% endmacro %}

{% macro snowflake__scalar_function_volatility_sql() %}
    {% set volatility = model.config.get('volatility') %}
    {% if volatility == 'deterministic' %}
        IMMUTABLE
    {% elif volatility == 'non-deterministic'%}
        VOLATILE
    {% elif volatility == 'stable' or volatility != none %}
        {% do unsupported_volatility_warning(volatility) %}
    {% endif %}
    {# If no volatility is set, don't add anything and let the data warehouse default it #}
{% endmacro %}

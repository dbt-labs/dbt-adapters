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
    {% if model.config.get('volatility') == 'deterministic' %}
        IMMUTABLE
    {% elif model.config.get('volatility') == 'stable' %}
        {% do exceptions.raise_compiler_error("`Stable` function volatility is not supported for Snowflake") %}
    {% elif model.config.get('volatility') == 'non-deterministic' %}
        VOLATILE
    {% endif %}
    {# If no volatility is set, don't add anything and let the data warehouse default it #}
{% endmacro %}

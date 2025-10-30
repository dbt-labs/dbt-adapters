{% macro redshift__scalar_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql()}})
        RETURNS {{ model.returns.data_type }}
    {{ scalar_function_volatility_sql() }}
    AS
{% endmacro %}

{% macro redshift__formatted_scalar_function_args_sql() %}
    {% set args = [] %}
    {% for arg in model.arguments -%}
        {%- do args.append(arg.data_type) -%}
    {%- endfor %}
    {{ args | join(', ') }}
{% endmacro %}

{% macro redshift__scalar_function_volatility_sql() %}
    {% if model.config.get('volatility') == 'deterministic' %}
        IMMUTABLE
    {% elif model.config.get('volatility') == 'stable' %}
        STABLE
    {% else %}
        {# At this point, either they've set `non-deterministic` or they've set nothing. In either case, we default to VOLATILE #}
        VOLATILE
    {% endif %}
{% endmacro %}

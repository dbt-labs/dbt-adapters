{% macro bigquery__get_aggregate_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE AGGREGATE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql()}})
    RETURNS {{ model.returns.data_type }}
    {{ scalar_function_volatility_sql() }}
    AS
{% endmacro %}

{% macro bigquery__aggregate_function_sql(target_relation) %}
    {{ bigquery__get_aggregate_function_create_replace_signature_sql(target_relation) }}
    {# conviently we can reuse the sql scalar function body #}
    {{ scalar_function_body_sql() }}
{% endmacro %}

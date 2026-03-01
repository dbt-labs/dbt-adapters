{% macro bigquery__aggregate_function_sql(target_relation) %}
    {{ get_aggregate_function_create_replace_signature(target_relation) }}
    {# conveniently we can reuse the sql scalar function body #}
    {{ scalar_function_body_sql() }}
{% endmacro %}

{% macro bigquery__aggregate_function_javascript(target_relation) %}
    {{ get_aggregate_function_create_replace_signature(target_relation) }}
    {# conveniently we can reuse the sql scalar function body #}
    {{ bigquery__get_scalar_function_body_javascript() }}
{% endmacro %}

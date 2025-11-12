{% macro snowflake__get_aggregate_function_create_replace_signature_python(target_relation) %}
    CREATE OR REPLACE AGGREGATE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql()}})
    RETURNS {{ model.returns.data_type }}
    LANGUAGE PYTHON
    RUNTIME_VERSION = '{{ model.config.get('runtime_version') }}'
    HANDLER = '{{ model.config.get('entry_point') }}'
    {{ scalar_function_volatility_sql() }}
    AS
{% endmacro %}

{% macro snowflake__aggregate_function_python(target_relation) %}
    {{ snowflake__get_aggregate_function_create_replace_signature_python(target_relation) }}
    {# convieniently we can reuse the sql scalar function body #}
    {{ scalar_function_body_sql() }}
{% endmacro %}

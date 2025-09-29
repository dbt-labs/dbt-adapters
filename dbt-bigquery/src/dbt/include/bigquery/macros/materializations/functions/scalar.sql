{% macro bigquery__scalar_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql()}})
    RETURNS {{ model.return_type.type }}
    AS
{% endmacro %}

{% macro bigquery__scalar_function_body_sql() %}
    (
       {{ model.compiled_code }}
    )
{% endmacro %}

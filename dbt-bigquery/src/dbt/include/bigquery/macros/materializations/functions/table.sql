{% macro bigquery__table_function_sql(target_relation) %}
    {{ table_function_create_replace_signature_sql(target_relation) }}
    {{ table_function_body_sql() }}
{% endmacro %}

{% macro bigquery__table_function_body_sql() %}
    (
       {{ model.compiled_code }}
    )
{% endmacro %}

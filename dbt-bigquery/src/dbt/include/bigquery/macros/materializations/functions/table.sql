{% macro bigquery__table_function_body_sql() %}
    (
       {{ model.compiled_code }}
    )
{% endmacro %}

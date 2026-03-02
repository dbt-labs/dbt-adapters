{% macro bigquery__table_function_sql(target_relation) %}
    {{ bigquery__table_function_create_replace_signature_sql(target_relation) }}
    {{ bigquery__table_function_body_sql() }}
{% endmacro %}

{% macro bigquery__table_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE TABLE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql() }})
    AS
{% endmacro %}

{% macro bigquery__table_function_body_sql() %}
    (
       {{ model.compiled_code }}
    )
{% endmacro %}

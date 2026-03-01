{% macro table_function_sql(target_relation) %}
    {{ return(adapter.dispatch('table_function_sql', 'dbt')(target_relation)) }}
{% endmacro %}

{% macro default__table_function_sql(target_relation) %}
    {{ table_function_create_replace_signature_sql(target_relation) }}
    {{ table_function_body_sql() }};
{% endmacro %}

{% macro table_function_create_replace_signature_sql(target_relation) %}
    {{ return(adapter.dispatch('table_function_create_replace_signature_sql', 'dbt')(target_relation)) }}
{% endmacro %}

{% macro default__table_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE TABLE FUNCTION {{ target_relation.render() }} ({{ formatted_table_function_args_sql() }})
    AS
{% endmacro %}

{% macro formatted_table_function_args_sql() %}
    {{ return(adapter.dispatch('formatted_table_function_args_sql', 'dbt')()) }}
{% endmacro %}

{% macro default__formatted_table_function_args_sql() %}
    {# reuse the scalar function args formatting #}
    {{ formatted_scalar_function_args_sql() }}
{% endmacro %}

{% macro table_function_body_sql() %}
    {{ return(adapter.dispatch('table_function_body_sql', 'dbt')()) }}
{% endmacro %}

{% macro default__table_function_body_sql() %}
    $$
       {{ model.compiled_code }}
    $$ LANGUAGE SQL
{% endmacro %}

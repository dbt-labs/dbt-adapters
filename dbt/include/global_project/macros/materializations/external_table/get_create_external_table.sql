{% macro create_external_table(relation, columns) %}
    {{ adapter.dispatch('create_external_table', 'dbt')(relation, columns) }}
{% endmacro %}

{% macro default__create_external_table(relation, columns) %}
    {{ exceptions.raise_compiler_error("External table creation is not implemented for the default adapter") }}
{% endmacro %}

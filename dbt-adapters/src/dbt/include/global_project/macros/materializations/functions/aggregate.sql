{% macro function_aggregate_sql(target_relation) %}
    {{ get_function_aggregate_create_replace_signature(target_relation) }}
    {{ get_function_aggregate_body() }};
{% endmacro %}

{% macro get_function_aggregate_create_replace_signature(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ get_formatted_function_aggregate_args()}}) RETURNS {{ model.return_type.type }} AS
{% endmacro %}

{% macro get_formatted_function_aggregate_args() %}
    {% set args = [] %}
    {% for arg in model.arguments -%}
        {%- do args.append(arg.name ~ ' ' ~ arg.type) -%}
    {%- endfor %}
    {{ args | join(', ') }}
{% endmacro %}

{% macro get_function_aggregate_body() %}
    $$
       {{ model.compiled_code }}
    $$ LANGUAGE SQL;
{% endmacro %}

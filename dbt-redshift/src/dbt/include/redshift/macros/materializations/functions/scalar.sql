{% macro redshift__formatted_scalar_function_args_sql() %}
    {% set args = [] %}
    {% for arg in model.arguments -%}
        {%- do args.append(arg.type) -%}
    {%- endfor %}
    {{ args | join(', ') }}
{% endmacro %}

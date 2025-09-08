{% macro get_udf_build_sql(target_relation) %}
    {{ get_udf_create_replace_signature(target_relation) }}
    {{ get_udf_body() }};
{% endmacro %}

{% macro get_udf_create_replace_signature(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ get_formatted_udf_args()}}) RETURNS {{ model.return_type.type }} AS
{% endmacro %}

{% macro get_formatted_udf_args() %}
    {% set args = [] %}
    {% for arg in model.arguments -%}
        {%- do args.append(arg.name ~ ' ' ~ arg.type) -%}
    {%- endfor %}
    {{ args | join(', ') }}
{% endmacro %}

{% macro get_udf_body() %}
    $$
       {{ model.compiled_code }}
    $$ LANGUAGE SQL;
{% endmacro %}

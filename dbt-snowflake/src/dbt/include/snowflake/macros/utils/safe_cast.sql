{% macro snowflake__safe_cast(field, type) %}
    {% if type|upper == "GEOMETRY" -%}
        try_to_geometry({{field}})
    {% elif type|upper == "GEOGRAPHY" -%}
        try_to_geography({{field}})
    {% elif type|upper != "VARIANT" -%}
        {#-- Snowflake try_cast does not support casting to variant, and expects the field as a string --#}
        {%- if field is mapping or (field is sequence and field is not string) -%}
            {#-- A dict or list field's default rendering is Python's own str(),
                 which spells a nested null as None rather than the null Snowflake's
                 object/array constructor syntax expects, so it needs its own
                 serialization instead of falling straight into {{field}} below --#}
            {% set field_as_string = snowflake_composite_literal(field) %}
        {%- else -%}
            {% set field_as_string =  dbt.string_literal(field) if field is number else field %}
        {%- endif -%}
        try_cast({{field_as_string}} as {{type}})
    {% else -%}
        {{ adapter.dispatch('cast', 'dbt')(field, type) }}
    {% endif -%}
{% endmacro %}

{%- macro snowflake_composite_literal(value) -%}
    {#-- Renders a dict or list as Snowflake's own object/array constructor
         syntax, the same shape Python's str() already produces for
         everything except None, which this corrects to null at any nesting
         depth. --#}
    {%- if value is none -%}
        {{- 'null' -}}
    {%- elif value is mapping -%}
        {%- set parts = [] -%}
        {%- for key, item in value.items() -%}
            {%- do parts.append(dbt.string_literal(dbt.escape_single_quotes(key)) ~ ': ' ~ snowflake_composite_literal(item)) -%}
        {%- endfor -%}
        {{- '{' ~ parts | join(', ') ~ '}' -}}
    {%- elif value is sequence and value is not string -%}
        {%- set parts = [] -%}
        {%- for item in value -%}
            {%- do parts.append(snowflake_composite_literal(item)) -%}
        {%- endfor -%}
        {{- '[' ~ parts | join(', ') ~ ']' -}}
    {%- elif value is string -%}
        {{- dbt.string_literal(dbt.escape_single_quotes(value)) -}}
    {%- else -%}
        {{- value -}}
    {%- endif -%}
{%- endmacro -%}

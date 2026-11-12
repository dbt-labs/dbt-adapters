{#- try_parse_json returns null for invalid JSON. The cast returns text, not a variant,
    to match other adapters. -#}
{% macro snowflake__json_parse(string, string_path) -%}

    cast(try_parse_json({{ string }}) {%- for s in string_path -%}['{{ s }}']{%- endfor %} as {{ dbt.type_string() }})

{%- endmacro %}

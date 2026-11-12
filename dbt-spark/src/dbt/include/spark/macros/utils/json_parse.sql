{#- get_json_object works on both Apache Spark and Databricks and returns null for invalid JSON. -#}
{% macro spark__json_parse(string, string_path) -%}

    get_json_object({{ string }}, '$.{%- for s in string_path -%}{{ s }}{%- if not loop.last -%}.{%- endif -%}{%- endfor -%}')

{%- endmacro %}

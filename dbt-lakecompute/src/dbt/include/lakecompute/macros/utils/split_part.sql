{#- Ported from dbt-duckdb (Apache-2.0): macros/utils/splitpart.sql -#}

{% macro lakecompute__split_part(string_text, delimiter_text, part_number) %}
    {#- negative indexes count from the end, matching dbt's split_part contract -#}
    string_split({{ string_text }}, {{ delimiter_text }})[ {{ part_number }} ]
{% endmacro %}

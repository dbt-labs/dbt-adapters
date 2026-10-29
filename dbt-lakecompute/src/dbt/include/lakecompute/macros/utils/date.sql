{#- Ported from dbt-duckdb (Apache-2.0): macros/utils/date.sql -#}

{% macro lakecompute__date(year, month, day) -%}
    make_date({{ year }}, {{ month }}, {{ day }})
{%- endmacro %}

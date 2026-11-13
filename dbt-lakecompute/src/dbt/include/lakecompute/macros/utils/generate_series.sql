{#- Ported from dbt-duckdb (Apache-2.0): macros/utils/generate_series.sql -#}

{% macro lakecompute__generate_series(upper_bound) %}
    select
        generate_series as generated_number
    from generate_series(1, {{ upper_bound }})
{% endmacro %}

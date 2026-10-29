{#- Ported from dbt-duckdb (Apache-2.0): macros/utils/dateadd.sql -#}

{% macro lakecompute__dateadd(datepart, interval, from_date_or_timestamp) %}

    {#- multiply an interval so `interval` can be an expression; no `quarter` unit exists -#}

    {%- set unit = datepart | lower -%}
    {%- if unit == 'quarter' -%}
        ({{ from_date_or_timestamp }} + (cast({{ interval }} as bigint) * 3) * interval 1 month)
    {%- elif unit == 'week' -%}
        ({{ from_date_or_timestamp }} + (cast({{ interval }} as bigint) * 7) * interval 1 day)
    {%- else -%}
        ({{ from_date_or_timestamp }} + cast({{ interval }} as bigint) * interval 1 {{ unit }})
    {%- endif -%}

{% endmacro %}

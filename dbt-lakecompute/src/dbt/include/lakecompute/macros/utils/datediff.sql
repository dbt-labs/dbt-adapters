{#- Ported from dbt-duckdb (Apache-2.0): macros/utils/datediff.sql -#}
{#- Equal weekdays get no correction: upstream subtracts 1 when first_date is later, so
    datediff(date '2024-01-15', date '2024-01-01', 'week') would be -3 instead of -2. -#}

{% macro lakecompute__datediff(first_date, second_date, datepart) -%}
    {% if datepart == 'week' %}
            ({{ datediff(first_date, second_date, 'day') }} // 7 + case
            when date_part('dow', ({{ first_date }})::timestamp) < date_part('dow', ({{ second_date }})::timestamp) then
                case when {{ first_date }} <= {{ second_date }} then 0 else -1 end
            when date_part('dow', ({{ first_date }})::timestamp) > date_part('dow', ({{ second_date }})::timestamp) then
                case when {{ first_date }} <= {{ second_date }} then 1 else 0 end
            else 0
        end)
    {% else %}
        (date_diff('{{ datepart }}', {{ first_date }}::timestamp, {{ second_date }}::timestamp ))
    {% endif %}
{%- endmacro %}

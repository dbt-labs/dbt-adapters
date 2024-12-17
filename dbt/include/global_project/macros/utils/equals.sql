{% macro equals(first_date, second_date, datepart) %}
    {{ return(adapter.dispatch('equals', 'dbt') (expr1, expr2)) }}
{%- endmacro %}

{% macro default__equals(expr1, expr2) -%}

    case when (({{ expr1 }} = {{ expr2 }}) or ({{ expr1 }} is null and {{ expr2 }} is null))
        then 0
        else 1
    end = 0

{% endmacro %}
# If date_spine works properly, there should be no `null` values in the resulting model

models__test_date_spine_sql = """
with generated_dates as (
    {% if target.type == 'postgres' %}
        {{ date_spine("day", "'2023-09-07'::date", "'2023-09-10'::date") }}

    {% elif target.type == 'bigquery' or target.type == 'redshift' %}
        select cast(date_day as date) as date_day
        from ({{ date_spine("day", "'2023-09-07'", "'2023-09-10'") }})

    {% else %}
        {{ date_spine("day", "'2023-09-07'", "'2023-09-10'") }}
    {% endif %}
), expected_dates as (
    {% if target.type == 'postgres' %}
        select '2023-09-07'::date as expected
        union all
        select '2023-09-08'::date as expected
        union all
        select '2023-09-09'::date as expected

    {% elif target.type == 'bigquery' or target.type == 'redshift' %}
        select cast('2023-09-07' as date) as expected
        union all
        select cast('2023-09-08' as date) as expected
        union all
        select cast('2023-09-09' as date) as expected

    {% else %}
        select '2023-09-07' as expected
        union all
        select '2023-09-08' as expected
        union all
        select '2023-09-09' as expected
    {% endif %}
), joined as (
    select
        generated_dates.date_day,
        expected_dates.expected
    from generated_dates
    full outer join expected_dates on generated_dates.date_day = expected_dates.expected
)

SELECT * from joined
"""

models__test_date_spine_yml = """
version: 2
models:
  - name: test_date_spine
    data_tests:
      - assert_equal:
          actual: date_day
          expected: expected
"""

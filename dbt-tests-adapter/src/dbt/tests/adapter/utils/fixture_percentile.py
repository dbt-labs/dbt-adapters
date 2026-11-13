# percentile

seeds__data_percentile_csv = """key_column,value_field
a,1
a,2
a,3
a,4
b,10
c,1
c,3
"""


seeds__data_percentile_expected_csv = """key_column,value
a,2.5
b,10
c,2
"""


# percentile is a window function on most adapters, so each row gets its partition's value.
models__test_percentile_sql = """
with data as (

    select * from {{ ref('data_percentile') }}

),

data_output as (

    select * from {{ ref('data_percentile_expected') }}

),

calculate as (

    select distinct
        key_column,
        {{ percentile('value_field', 'key_column', '0.5') }} as value
    from data

)

select
    calculate.value as actual,
    data_output.value as expected
from calculate
left join data_output
on calculate.key_column = data_output.key_column
"""


# Postgres doesn't support percentile_cont as a window function, so postgres__percentile is an
# aggregate and needs a group by.
models__test_percentile_aggregate_sql = """
with data as (

    select * from {{ ref('data_percentile') }}

),

data_output as (

    select * from {{ ref('data_percentile_expected') }}

),

calculate as (

    select
        key_column,
        {{ percentile('value_field', 'key_column', '0.5') }} as value
    from data
    group by key_column

)

select
    calculate.value as actual,
    data_output.value as expected
from calculate
left join data_output
on calculate.key_column = data_output.key_column
"""


models__test_percentile_yml = """
version: 2
models:
  - name: test_percentile
    data_tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# concat

# https://github.com/dbt-labs/dbt-core/issues/4725
seeds__data_concat_csv = """input_1,input_2,output
a,b,ab
a,EMPTY,a
EMPTY,b,b
EMPTY,EMPTY,EMPTY
"""


models__test_concat_sql = """
with seed_data as (

    select * from {{ ref('data_concat') }}

),

data as (

    select
        {{ replace_empty('input_1') }} as input_1,
        {{ replace_empty('input_2') }} as input_2,
        {{ replace_empty('output') }} as output
    from seed_data

)

select
    {{ concat(['input_1', 'input_2']) }} as actual,
    output as expected

from data
"""


models__test_concat_yml = """
version: 2
models:
  - name: test_concat
    data_tests:
      - assert_equal:
          actual: actual
          expected: expected
"""


# Single-field concat: many adapters override default__concat with a SQL CONCAT()
# function call, and CONCAT() requires at least two arguments on engines like
# SQL Server / Fabric T-SQL. The default macro therefore short-circuits on a
# single-element list and returns the field directly.
models__test_concat_single_field_sql = """
with seed_data as (

    select * from {{ ref('data_concat') }}

),

data as (

    select
        {{ replace_empty('input_1') }} as input_1
    from seed_data

)

select
    {{ concat(['input_1']) }} as actual,
    input_1 as expected

from data
"""


models__test_concat_single_field_yml = """
version: 2
models:
  - name: test_concat_single_field
    data_tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# cast

seeds__data_cast_csv = """field,output
abc,abc
123,123
,
"""


models__test_cast_sql = """
with data as (

    select * from {{ ref('data_cast') }}

)

select
    {{ cast('field', api.Column.translate_type('string')) }} as actual,
    output as expected

from data
"""


models__test_cast_yml = """
version: 2
models:
  - name: test_cast
    data_tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

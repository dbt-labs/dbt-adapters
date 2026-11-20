# json_parse

seeds__data_json_parse_csv = """key_column,json_string
1,"{""name"": ""Ann"", ""address"": {""city"": ""Oakland""}}"
2,"{""name"": ""Bo""}"
"""


seeds__data_json_parse_expected_csv = """key_column,name,city
1,Ann,Oakland
2,Bo,
"""


models__test_json_parse_sql = """
with data as (

    select * from {{ ref('data_json_parse') }}

),

data_output as (

    select * from {{ ref('data_json_parse_expected') }}

),

calculate as (

    select
        key_column,
        {{ json_parse('json_string', ['name']) }} as name,
        {{ json_parse('json_string', ['address', 'city']) }} as city
    from data

)

select
    calculate.name as actual_name,
    data_output.name as expected_name,
    calculate.city as actual_city,
    data_output.city as expected_city
from calculate
left join data_output
on calculate.key_column = data_output.key_column
"""


models__test_json_parse_yml = """
version: 2
models:
  - name: test_json_parse
    data_tests:
      - assert_equal:
          actual: actual_name
          expected: expected_name
      - assert_equal:
          actual: actual_city
          expected: expected_city
"""

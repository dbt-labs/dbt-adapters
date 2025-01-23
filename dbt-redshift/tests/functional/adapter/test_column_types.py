import pytest
from dbt.tests.adapter.column_types.test_column_types import BaseColumnTypes

_MODEL_SQL = """
select
    1::smallint as smallint_col,
    2::int as int_col,
    3::bigint as bigint_col,
    4::int2 as int2_col,
    5::int4 as int4_col,
    6::int8 as int8_col,
    7::integer as integer_col,
    8.0::real as real_col,
    9.0::float4 as float4_col,
    10.0::float8 as float8_col,
    11.0::float as float_col,
    12.0::double precision as double_col,
    13.0::numeric as numeric_col,
    14.0::decimal as decimal_col,
    '15'::varchar(20) as varchar_col,
    '16'::text as text_col
"""

_SCHEMA_YML = """
version: 2
models:
  - name: model
    tests:
      - is_type:
          column_map:
            smallint_col: ['integer', 'number']
            int_col: ['integer', 'number']
            bigint_col: ['integer', 'number']
            int2_col: ['integer', 'number']
            int4_col: ['integer', 'number']
            int8_col: ['integer', 'number']
            integer_col: ['integer', 'number']
            real_col: ['float', 'number']
            double_col: ['float', 'number']
            float4_col: ['float', 'number']
            float8_col: ['float', 'number']
            float_col: ['float', 'number']
            numeric_col: ['numeric', 'number']
            decimal_col: ['numeric', 'number']
            varchar_col: ['string', 'not number']
            text_col: ['string', 'not number']
"""


class TestRedshiftColumnTypes(BaseColumnTypes):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": _MODEL_SQL, "schema.yml": _SCHEMA_YML}

    def test_run_and_test(self, project):
        self.run_and_test()

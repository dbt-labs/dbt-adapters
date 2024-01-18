model_sql = """
select
    1::smallint as smallint_col,
    2::integer as int_col,
    3::bigint as bigint_col,
    4.0::real as real_col,
    5.0::double precision as double_col,
    6.0::numeric as numeric_col,
    '7'::text as text_col,
    '8'::varchar(20) as varchar_col
"""

schema_yml = """
version: 2
models:
  - name: model
    data_tests:
      - is_type:
          column_map:
            smallint_col: ['integer', 'number']
            int_col: ['integer', 'number']
            bigint_col: ['integer', 'number']
            real_col: ['float', 'number']
            double_col: ['float', 'number']
            numeric_col: ['numeric', 'number']
            text_col: ['string', 'not number']
            varchar_col: ['string', 'not number']
"""

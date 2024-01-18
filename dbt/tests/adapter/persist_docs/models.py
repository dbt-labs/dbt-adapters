VIEW = """
{{ config(materialized='view') }}
select 2 as id, 'Bob' as name
"""


NO_DOCS_MODEL = """
select 1 as id, 'Alice' as name
"""


TABLE = """
{{ config(materialized='table') }}
select 1 as id, 'Joe' as name
"""


MISSING_COLUMN = """
{{ config(materialized='table') }}
select 1 as id, 'Ed' as name
"""


MODEL_USING_QUOTE_UTIL = """
select 1 as {{ adapter.quote("2id") }}
"""

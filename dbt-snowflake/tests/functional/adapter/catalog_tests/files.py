MY_SEED = """
id,value
1,100
2,200
3,300
""".strip()


MY_TABLE = """
{{ config(
    materialized='table',
) }}
select * from {{ ref('my_seed') }}
"""


MY_VIEW = """
{{ config(
    materialized='view',
) }}
select * from {{ ref('my_seed') }}
"""


MY_DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='30 minutes',
) }}
select * from {{ ref('my_seed') }}
"""


MY_NUMERIC_TABLE = """
{{ config(
    materialized='table',
) }}
select
    cast(901.75 as number(12,2)) as retail_price,
    cast(42 as number(38,0)) as whole_number,
    cast('hello' as varchar(16)) as label
"""

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


MY_TABLE_IN_ANOTHER_SCHEMA = """
{{ config(
    materialized='table',
    schema='another',
) }}
select * from {{ ref('my_seed') }}
"""


MY_VIEW_IN_ANOTHER_SCHEMA = """
{{ config(
    materialized='view',
    schema='another',
) }}
select * from {{ ref('my_seed') }}
"""

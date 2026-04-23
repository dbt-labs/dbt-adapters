SEED = """
id,value
1,100
2,200
3,300
""".strip()


# Static interactive table: cluster_by only, no target_lag / warehouse.
INTERACTIVE_TABLE_STATIC = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
) }}
select id, value from {{ ref('my_seed') }}
"""


# Dynamic interactive table: cluster_by + target_lag + snowflake_warehouse.
INTERACTIVE_TABLE_DYNAMIC = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
    target_lag='2 minutes',
    snowflake_warehouse='DBT_TESTING',
) }}
select id, value from {{ ref('my_seed') }}
"""


# Altered cluster_by for config-change detection.
INTERACTIVE_TABLE_STATIC_CLUSTER_ALTER = """
{{ config(
    materialized='interactive_table',
    cluster_by='value',
) }}
select id, value from {{ ref('my_seed') }}
"""


# Altered target_lag.
INTERACTIVE_TABLE_DYNAMIC_LAG_ALTER = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
    target_lag='5 minutes',
    snowflake_warehouse='DBT_TESTING',
) }}
select id, value from {{ ref('my_seed') }}
"""


# Missing cluster_by should produce a compilation error.
INTERACTIVE_TABLE_MISSING_CLUSTER_BY = """
{{ config(
    materialized='interactive_table',
) }}
select id, value from {{ ref('my_seed') }}
"""


# target_lag without snowflake_warehouse should produce a compilation error.
INTERACTIVE_TABLE_LAG_NO_WAREHOUSE = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
    target_lag='2 minutes',
) }}
select id, value from {{ ref('my_seed') }}
"""

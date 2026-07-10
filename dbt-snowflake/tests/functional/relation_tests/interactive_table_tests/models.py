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


# Static interactive table with altered SQL (fewer rows) to prove a rebuild every run
# reflects source/SQL changes even though no config field changed.
INTERACTIVE_TABLE_STATIC_SQL_ALTER = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
) }}
select id, value from {{ ref('my_seed') }} where id <= 2
"""


# A plain table, used to test converting an existing relation into an interactive table.
TABLE_RELATION = """
{{ config(materialized='table') }}
select id, value from {{ ref('my_seed') }}
"""


# Dynamic interactive table: cluster_by + target_lag + snowflake_warehouse.
INTERACTIVE_TABLE_DYNAMIC = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
    target_lag='2 minutes',
    snowflake_warehouse=env_var('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TESTING'),
) }}
select id, value from {{ ref('my_seed') }}
"""


# Dynamic interactive table with a multi-column cluster_by. Guards the no-op round-trip:
# SHOW INTERACTIVE TABLES returns the clustering key parenthesized, e.g. '(id, value)'.
INTERACTIVE_TABLE_DYNAMIC_MULTICOL = """
{{ config(
    materialized='interactive_table',
    cluster_by=['id', 'value'],
    target_lag='2 minutes',
    snowflake_warehouse=env_var('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TESTING'),
) }}
select id, value from {{ ref('my_seed') }}
"""


# Dynamic interactive table with an altered cluster_by, for config-change detection.
INTERACTIVE_TABLE_DYNAMIC_CLUSTER_ALTER = """
{{ config(
    materialized='interactive_table',
    cluster_by='value',
    target_lag='2 minutes',
    snowflake_warehouse=env_var('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TESTING'),
) }}
select id, value from {{ ref('my_seed') }}
"""


# Altered target_lag.
INTERACTIVE_TABLE_DYNAMIC_LAG_ALTER = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
    target_lag='5 minutes',
    snowflake_warehouse=env_var('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TESTING'),
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


# Static interactive table attached to one or more interactive warehouses. The list is
# provided via env var (comma-separated) so tests can point at warehouses they provision.
INTERACTIVE_TABLE_STATIC_ATTACH = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
    snowflake_interactive_warehouses=env_var('SNOWFLAKE_TEST_INTERACTIVE_WHS').split(','),
) }}
select id, value from {{ ref('my_seed') }}
"""


# Dynamic interactive table with an attachment list, to prove attach runs on a no-op rerun.
INTERACTIVE_TABLE_DYNAMIC_ATTACH = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
    target_lag='2 minutes',
    snowflake_warehouse=env_var('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TESTING'),
    snowflake_interactive_warehouses=env_var('SNOWFLAKE_TEST_INTERACTIVE_WHS').split(','),
) }}
select id, value from {{ ref('my_seed') }}
"""

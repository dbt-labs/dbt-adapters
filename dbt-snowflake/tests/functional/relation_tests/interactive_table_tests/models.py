SEED = """
id,value
1,100
2,200
3,300
""".strip()


# A static interactive table: no target_lag, no warehouse -- Snowflake never
# auto-refreshes it and never accepts a warehouse for it. `cluster_by` is
# required on every interactive table, static or dynamic.
INTERACTIVE_TABLE_STATIC = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
) }}
select id, value from {{ ref('my_seed') }}
"""


# A dynamic interactive table: target_lag + warehouse make it self-refreshing.
INTERACTIVE_TABLE_DYNAMIC = """
{{ config(
    materialized='interactive_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='1 hour',
    cluster_by='id',
) }}
select id, value from {{ ref('my_seed') }}
"""


# target_lag value-to-value change -- alterable in place.
INTERACTIVE_TABLE_DYNAMIC_TARGET_LAG_ALTER = """
{{ config(
    materialized='interactive_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 hours',
    cluster_by='id',
) }}
select id, value from {{ ref('my_seed') }}
"""


# `refresh_warehouse` explicitly overrides the table's self-refresh warehouse;
# `snowflake_warehouse` stays the same as INTERACTIVE_TABLE_DYNAMIC so only the
# refresh warehouse itself changes.
INTERACTIVE_TABLE_DYNAMIC_REFRESH_WAREHOUSE_ALTER = """
{{ config(
    materialized='interactive_table',
    snowflake_warehouse='DBT_TESTING',
    refresh_warehouse=env_var('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TESTING'),
    target_lag='1 hour',
    cluster_by='id',
) }}
select id, value from {{ ref('my_seed') }}
"""


# cluster_by change -- Snowflake rejects ALTER ... CLUSTER BY on an interactive
# table (001003), so this must force a full CREATE OR REPLACE.
INTERACTIVE_TABLE_DYNAMIC_CLUSTER_BY_ALTER = """
{{ config(
    materialized='interactive_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='1 hour',
    cluster_by='value',
) }}
select id, value from {{ ref('my_seed') }}
"""


# snowflake_initialization_warehouse fixtures.
INTERACTIVE_TABLE_DYNAMIC_WITH_INIT_WAREHOUSE = """
{{ config(
    materialized='interactive_table',
    snowflake_warehouse='DBT_TESTING',
    snowflake_initialization_warehouse=env_var('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TESTING'),
    target_lag='1 hour',
    cluster_by='id',
) }}
select id, value from {{ ref('my_seed') }}
"""


INTERACTIVE_TABLE_DYNAMIC_WITH_INIT_WAREHOUSE_ALTER = """
{{ config(
    materialized='interactive_table',
    snowflake_warehouse='DBT_TESTING',
    snowflake_initialization_warehouse='DBT_TESTING',
    target_lag='1 hour',
    cluster_by='id',
) }}
select id, value from {{ ref('my_seed') }}
"""


# Same shape as INTERACTIVE_TABLE_DYNAMIC -- no snowflake_initialization_warehouse
# key at all -- used as the "unset" target for the init-warehouse changeset tests.
INTERACTIVE_TABLE_DYNAMIC_WITHOUT_INIT_WAREHOUSE = INTERACTIVE_TABLE_DYNAMIC


# --- Compile-time validation fixtures ---
# Each of these must fail at `dbt run` with a CompilationError before any SQL
# reaches Snowflake -- see SnowflakeInteractiveTableConfig.parse_relation_config.

INTERACTIVE_TABLE_MISSING_CLUSTER_BY = """
{{ config(
    materialized='interactive_table',
) }}
select 1 as id
"""


INTERACTIVE_TABLE_BLANK_CLUSTER_BY = """
{{ config(
    materialized='interactive_table',
    cluster_by='',
) }}
select 1 as id
"""


INTERACTIVE_TABLE_ICEBERG_FORMAT = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
    table_format='iceberg',
) }}
select 1 as id
"""


INTERACTIVE_TABLE_TRANSIENT_TRUE = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
    transient=true,
) }}
select 1 as id
"""


INTERACTIVE_TABLE_TARGET_LAG_NO_WAREHOUSE = """
{{ config(
    materialized='interactive_table',
    cluster_by='id',
    target_lag='1 hour',
) }}
select 1 as id
"""

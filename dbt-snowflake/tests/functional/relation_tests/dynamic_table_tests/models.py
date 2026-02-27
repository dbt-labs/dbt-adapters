SEED = """
id,value
1,100
2,200
3,300
""".strip()


# Seed with a column named "NONE" to test edge case where column name
# matches the string Snowflake uses for unset values
SEED_WITH_NONE_COLUMN = """
id,NONE
1,100
2,200
3,300
""".strip()


DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""


EXPLICIT_AUTO_DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='AUTO',
) }}
select * from {{ ref('my_seed') }}
"""

IMPLICIT_AUTO_DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_DOWNSTREAM = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='DOWNSTREAM',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_ICEBERG_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_ALTER = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='5 minutes',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_REPLACE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='FULL',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_ICEBERG_TABLE_ALTER = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='5 minutes',
    refresh_mode='INCREMENTAL',
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_ICEBERG_TABLE_REPLACE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='FULL',
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
) }}
select * from {{ ref('my_seed') }}
"""

DYNAMIC_TABLE_CUSTOM_SCHEMA = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='30 minutes',
    schema='custom_schema'
) }}
select * from {{ ref('simple_model') }}
"""

DYNAMIC_TABLE_CUSTOM_DB_SCHEMA = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='30 minutes',
    schema='custom_schema'
) }}
select * from {{ ref('simple_model') }}
"""

DYNAMIC_TABLE_WITH_INIT_WAREHOUSE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    snowflake_initialization_warehouse=env_var('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TESTING'),
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""

DYNAMIC_TABLE_WITH_INIT_WAREHOUSE_ALTER = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    snowflake_initialization_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""

SIMPLE_MODEL = """
{{ config(
    materialized='table'
) }}
SELECT 1 as id
"""


# Immutable Where fixtures
DYNAMIC_TABLE_WITH_IMMUTABLE_WHERE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    immutable_where="id < 100",
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_WITH_IMMUTABLE_WHERE_ALTER = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    immutable_where="id < 50",
) }}
select * from {{ ref('my_seed') }}
"""


# Immutable Where with Jinja variable substitution
DYNAMIC_TABLE_WITH_IMMUTABLE_WHERE_JINJA = """
{%- set cutoff_value = var('immutable_cutoff', 100) -%}
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    immutable_where="id < " ~ cutoff_value,
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_WITH_IMMUTABLE_WHERE_JINJA_ALTER = """
{%- set cutoff_value = var('immutable_cutoff', 200) -%}
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    immutable_where="id < " ~ cutoff_value,
) }}
select * from {{ ref('my_seed') }}
"""


# UNSET fixtures - tables without optional fields for testing transitions from set -> unset
DYNAMIC_TABLE_WITHOUT_INIT_WAREHOUSE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_WITHOUT_IMMUTABLE_WHERE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""


# Cluster By fixtures
# Note: Snowflake requires clustered columns to be explicitly listed in the SELECT clause
# (cannot use SELECT * with cluster_by)
DYNAMIC_TABLE_WITH_CLUSTER_BY = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    cluster_by="id",
) }}
select id, value from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_WITH_CLUSTER_BY_MULTI = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    cluster_by=["id", "value"],
) }}
select id, value from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_WITH_CLUSTER_BY_ALTER = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    cluster_by="value",
) }}
select id, value from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_WITHOUT_CLUSTER_BY = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
) }}
select id, value from {{ ref('my_seed') }}
"""


# Test clustering by a column literally named "NONE" to ensure we don't
# incorrectly normalize it to Python None
DYNAMIC_TABLE_WITH_CLUSTER_BY_NONE_COLUMN = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    cluster_by='"NONE"',
) }}
select id, "NONE" from {{ ref('my_seed_none') }}
"""


# Transient dynamic table fixtures
DYNAMIC_TABLE_TRANSIENT = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    transient=True,
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_NON_TRANSIENT = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    transient=False,
) }}
select * from {{ ref('my_seed') }}
"""


# For testing default behavior (no explicit transient config)
DYNAMIC_TABLE_DEFAULT_TRANSIENT = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""

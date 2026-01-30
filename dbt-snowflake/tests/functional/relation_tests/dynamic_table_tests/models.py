SEED = """
id,value
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
    snowflake_initialization_warehouse='DBT_TESTING_ALT',
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

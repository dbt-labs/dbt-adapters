SEED = """
id,value
1,100
2,200
3,300
""".strip()


TABLE = """
{{ config(
    materialized='table',
) }}
select * from {{ ref('my_seed') }}
"""


VIEW = """
{{ config(
    materialized='view',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='1 minute',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_ICEBERG_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='1 minute',
    refresh_mode='INCREMENTAL',
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
) }}
select * from {{ ref('my_seed') }}
"""

ICEBERG_TABLE = """
{{ config(
    materialized='table',
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
) }}
select * from {{ ref('my_seed') }}
"""

INCREMENTAL_ICEBERG_TABLE = """
{{ config(
    materialized='incremental',
    table_format='iceberg',
    incremental_strategy='append',
    unique_key="id",
    external_volume = "s3_iceberg_snow",
) }}
select * from {{ ref('my_seed') }}
"""


INCREMENTAL_TABLE = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key="id",
) }}
select * from {{ ref('my_seed') }}
"""


HYBRID_TABLE = """
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'id': 'INT NOT NULL',
        'value': 'INT'
    },
    primary_key=['id']
) }}
select * from {{ ref('my_seed') }}
"""


INCREMENTAL_HYBRID_TABLE = """
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'id': 'INT NOT NULL',
        'value': 'INT'
    },
    primary_key=['id'],
    unique_key=['id']
) }}
select * from {{ ref('my_seed') }}
"""


HYBRID_TABLE_WITH_INDEX = """
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'id': 'INT NOT NULL',
        'value': 'INT'
    },
    primary_key=['id'],
    indexes=[
        {'name': 'idx_value', 'columns': ['value']}
    ]
) }}
select * from {{ ref('my_seed') }}
"""


HYBRID_TABLE_WITH_INDEX_INCLUDE = """
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'id': 'INT NOT NULL',
        'value': 'INT',
        'description': 'VARCHAR(200)'
    },
    primary_key=['id'],
    indexes=[
        {'name': 'idx_value_with_desc', 'columns': ['value'], 'include': ['description']}
    ]
) }}
select
    'test description' as description,
    id,
    value
from {{ ref('my_seed') }}
"""


HYBRID_TABLE_WITH_UNIQUE = """
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'id': 'INT NOT NULL',
        'value': 'INT NOT NULL',
        'code': 'VARCHAR(50) NOT NULL'
    },
    primary_key=['id'],
    unique_constraints=[
        {'name': 'uq_code', 'columns': ['code']}
    ]
) }}
select
    'CODE_' || id::VARCHAR as code,
    id,
    value
from {{ ref('my_seed') }}
"""


HYBRID_TABLE_WITH_COMMENT = """
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'id': 'INT NOT NULL',
        'value': 'INT'
    },
    primary_key=['id'],
    comment='Test hybrid table with comment for documentation'
) }}
select * from {{ ref('my_seed') }}
"""


HYBRID_TABLE_COMPOSITE_PK = """
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'id': 'INT NOT NULL',
        'partition_key': 'INT NOT NULL',
        'value': 'INT'
    },
    primary_key=['partition_key', 'id']
) }}
select
    id,
    1 as partition_key,
    value
from {{ ref('my_seed') }}
"""

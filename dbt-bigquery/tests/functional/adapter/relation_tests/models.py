import os

from tests.functional.utils import unique_prefix


# make sure this is static
PREFIX = unique_prefix()


SEED = """
id,value
1,100
2,200
3,300
""".strip()


VIEW = """
{{ config(
    materialized='view',
) }}
select * from {{ ref('my_seed') }}
"""


TABLE = """
{{ config(
    materialized='table',
) }}
select * from {{ ref('my_seed') }}
"""


MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view',
) }}
select * from {{ ref('my_seed') }}
"""


INCREMENTAL_TABLE = """
{{ config(
    materialized='incremental',
    unique_key="id",
) }}
select * from {{ ref('my_seed') }}
"""


ICEBERG_TABLE = (
    """
{{ config(
    materialized='table',
    catalog='managed_iceberg',
    storage_uri='gs://"""
    + os.getenv("BIGQUERY_TEST_ICEBERG_BUCKET")
    + """/"""
    + PREFIX
    + """__||storage_uri||'
) }}
select * from {{ ref('my_seed') }}
"""
)


INCREMENTAL_ICEBERG_TABLE = (
    """
{{ config(
    materialized='incremental',
    unique_key="id",
    catalog='managed_iceberg',
    storage_uri='gs://"""
    + os.getenv("BIGQUERY_TEST_ICEBERG_BUCKET")
    + """/"""
    + PREFIX
    + """__||storage_uri||'
) }}
select * from {{ ref('my_seed') }}
"""
)

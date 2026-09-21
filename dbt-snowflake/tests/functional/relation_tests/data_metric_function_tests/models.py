SEED = """
id,region
1,us
2,us
3,eu
""".strip()


# A table with a schedule and two system DMFs, one single-column and one that is
# declared with the list form of `expression`.
TABLE_TWO_DMFS = """
{{ config(
    materialized='table',
    data_metric_schedule='5 MINUTE',
    data_metric_functions=[
        {'metric': 'snowflake.core.null_count', 'expression': 'id'},
        {'metric': 'snowflake.core.duplicate_count', 'expression': ['id']},
    ],
) }}
select * from {{ ref('my_seed') }}
"""


# The same table converged down to a single DMF; used to assert that the removed
# DMF is dropped on the next run.
TABLE_ONE_DMF = """
{{ config(
    materialized='table',
    data_metric_schedule='5 MINUTE',
    data_metric_functions=[
        {'metric': 'snowflake.core.null_count', 'expression': 'id'},
    ],
) }}
select * from {{ ref('my_seed') }}
"""


# An incremental model carrying a DMF, to exercise the incremental materialization hook.
INCREMENTAL_ONE_DMF = """
{{ config(
    materialized='incremental',
    data_metric_schedule='5 MINUTE',
    data_metric_functions=[
        {'metric': 'snowflake.core.null_count', 'expression': 'id'},
    ],
) }}
select * from {{ ref('my_seed') }}
"""

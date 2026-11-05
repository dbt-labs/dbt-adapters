SEED = """
id,status,amount
1,completed,10.0
2,,-5.0
3,pending,7.5
""".strip()


def model(materialized: str, config: str = "") -> str:
    return f"""
{{{{ config(materialized='{materialized}'{config}) }}}}
select * from {{{{ ref('my_seed') }}}}
"""


NULL_COUNT_AND_ROW_COUNT = """,
    data_metric_schedule='5 MINUTE',
    data_metric_functions=[
        {'function': 'snowflake.core.null_count', 'columns': ['status']},
        {'function': 'snowflake.core.row_count', 'columns': []},
    ]"""

# NULL_COUNT moved from status to amount; ROW_COUNT dropped.
NULL_COUNT_ON_AMOUNT = """,
    data_metric_functions=[
        {'function': 'SNOWFLAKE.CORE.NULL_COUNT', 'columns': ['amount']},
    ]"""

ROW_COUNT_ONLY = """,
    data_metric_functions=[{'function': 'snowflake.core.row_count'}]"""

EMPTY_LIST = """,
    data_metric_functions=[]"""

TABLE = model("table", NULL_COUNT_AND_ROW_COUNT)
TABLE_REPLACED = model("table", NULL_COUNT_ON_AMOUNT)
VIEW = model("view", NULL_COUNT_AND_ROW_COUNT)
INCREMENTAL = model("incremental", NULL_COUNT_AND_ROW_COUNT)
INCREMENTAL_ROW_COUNT_ONLY = model("incremental", ROW_COUNT_ONLY)
INCREMENTAL_EMPTY_LIST = model("incremental", EMPTY_LIST)
INCREMENTAL_NO_CONFIG = model("incremental")
TABLE_INVALID_CONFIG = model("table", ", data_metric_schedule='5 HOURS'")


def dynamic_table(config: str = "") -> str:
    return f"""
{{{{ config(
    materialized='dynamic_table',
    snowflake_warehouse=target.warehouse,
    target_lag='2 minutes'{config}
) }}}}
select * from {{{{ ref('my_seed') }}}}
"""


DYNAMIC_TABLE = dynamic_table(NULL_COUNT_AND_ROW_COUNT)
DYNAMIC_TABLE_ROW_COUNT_ONLY = dynamic_table(ROW_COUNT_ONLY)


# Model-level bindings in schema YAML, including a pre-existing custom DMF.
YAML_MODEL = "select * from {{ ref('my_seed') }}"

MODEL_SCHEMA_YML = """
models:
  - name: my_model
    config:
      materialized: incremental
      data_metric_functions:
        - function: snowflake.core.row_count
        - function: snowflake.core.null_count
          columns: [status]
        - function: "{{ target.database }}.{{ target.schema }}.local_null_count"
          columns: [status]
        - function: snowflake.core.null_count
          columns: [amount]
"""

MODEL_SCHEMA_YML_REMOVED = """
models:
  - name: my_model
    config:
      materialized: incremental
      data_metric_functions:
        - function: snowflake.core.null_count
          columns: [status]
"""

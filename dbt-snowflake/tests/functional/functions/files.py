from dbt.tests.adapter.functions.files import MASK_PII_JS  # noqa: F401
from dbt.tests.adapter.functions.files import MY_JS_UDF  # noqa: F401
from dbt.tests.adapter.functions.files import SUM_POSITIVE_JS  # noqa: F401

MY_UDF_SQL = """
price * 2
""".strip()


# --- JavaScript UDF YAML configs (Snowflake-specific types) ---

MY_JS_UDF_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_WITH_DEFAULT_ARG_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: float
        default_value: 100
    returns:
      data_type: float
"""

MY_JS_UDF_SECURE_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      snowflake:
        secure: true
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_NULL_HANDLING_STRICT_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      snowflake:
        null_handling: strict
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_NULL_HANDLING_CALLED_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      snowflake:
        null_handling: called
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_LOG_LEVEL_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      snowflake:
        log_level: info
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_TRACE_LEVEL_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      snowflake:
        trace_level: "off"
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_ALL_CONFIGS_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      volatility: deterministic
      snowflake:
        secure: true
        null_handling: strict
        log_level: info
        trace_level: "off"
    arguments:
      - name: price
        data_type: float
        default_value: 100
    returns:
      data_type: float
"""

MY_JS_UDF_QUOTE_ARGS_FALSE_MULTI_ARG_YML = """
functions:
  - name: compute_total
    description: Multiplies price by quantity
    config:
      snowflake:
        quote_args: false
    arguments:
      - name: price
        data_type: float
      - name: quantity
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_QUOTE_ARGS_TRUE_MULTI_ARG_YML = """
functions:
  - name: compute_total
    description: Multiplies price by quantity
    config:
      snowflake:
        quote_args: true
    arguments:
      - name: price
        data_type: float
      - name: quantity
        data_type: float
    returns:
      data_type: float
"""

COMPUTE_TOTAL_JS_LOWERCASE = """
return price * quantity;
""".strip()

COMPUTE_TOTAL_JS_UPPERCASE = """
return PRICE * QUANTITY;
""".strip()

MASK_PII_JS_YML = """
functions:
  - name: mask_pii
    description: Masks all but the first 2 characters of a string
    arguments:
      - name: value
        data_type: varchar
    returns:
      data_type: varchar
"""

# JS aggregate UDF — not supported on Snowflake, should error
SUM_POSITIVE_JS_YML = """
functions:
  - name: sum_positive
    config:
      type: aggregate
    arguments:
      - name: x
        data_type: float
    returns:
      data_type: float
"""

MY_UDF_SQL = """
price * 2
""".strip()

MY_UDF_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: numeric
        description: The price of the standard item
    returns:
      data_type: numeric
      description: The resulting xlarge price
"""

MY_UDF_WITH_DEFAULT_ARG_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: numeric
        description: The price of the standard item
        default_value: 100
    returns:
      data_type: numeric
      description: The resulting xlarge price
"""

MY_UDF_PYTHON_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      entry_point: price_for_xlarge
      runtime_version: "3.11"
    arguments:
      - name: price
        data_type: numeric
        description: The price of the standard item
    returns:
      data_type: numeric
      description: The resulting xlarge price
"""

MY_UDF_PYTHON_WITH_PACKAGES_YML = """
functions:
  - name: sqrt_input
    description: Return the square root of the input using numpy
    config:
      entry_point: sqrt_input
      runtime_version: "3.11"
      packages: ["numpy"]
    arguments:
      - name: x
        data_type: FLOAT64
        description: The value to take the square root of
    returns:
      data_type: FLOAT64
      description: The square root of the input
""".strip()


# --- JavaScript UDF fixtures ---

MY_JS_UDF = """
return price * 2;
""".strip()

MY_JS_UDF_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: FLOAT64
    returns:
      data_type: FLOAT64
"""

MY_JS_UDF_WITH_DEFAULT_ARG_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: FLOAT64
        default_value: 100
    returns:
      data_type: FLOAT64
"""

# Snowflake-only configs that should be silently ignored on BigQuery
MY_JS_UDF_WITH_SNOWFLAKE_CONFIGS_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      snowflake:
        secure: true
        null_handling: strict
        log_level: info
        trace_level: "off"
    arguments:
      - name: price
        data_type: FLOAT64
    returns:
      data_type: FLOAT64
"""

MASK_PII_JS = """
if (value === null || value === undefined) {
    return null;
}
var masked = value.substring(0, 2);
for (var i = 2; i < value.length; i++) {
    masked += '*';
}
return masked;
""".strip()

MASK_PII_JS_YML = """
functions:
  - name: mask_pii
    description: Masks all but the first 2 characters of a string
    arguments:
      - name: value
        data_type: STRING
    returns:
      data_type: STRING
"""

# JS aggregate UDF — supported on BigQuery via CREATE AGGREGATE FUNCTION
SUM_POSITIVE_JS = """
export function initialState() {
  return {sum: 0}
}
export function aggregate(state, x) {
  if (x > 0) { state.sum += x; }
}
export function merge(state, partialState) {
  state.sum += partialState.sum;
}
export function finalize(state) {
  return state.sum;
}
""".strip()

SUM_POSITIVE_JS_YML = """
functions:
  - name: sum_positive
    description: Sums only the positive values
    config:
      type: aggregate
    arguments:
      - name: x
        data_type: FLOAT64
    returns:
      data_type: FLOAT64
"""

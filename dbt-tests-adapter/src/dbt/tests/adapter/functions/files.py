MY_UDF_SQL = """
SELECT price * 2
""".strip()

MY_UDF_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: float
        description: The price of the standard item
    returns:
      data_type: float
      description: The resulting xlarge price
"""

MY_UDF_PYTHON = """
def price_for_xlarge(price: float) -> float:
  return price * 2
"""

MY_UDF_PYTHON_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      entry_point: price_for_xlarge
      runtime_version: "3.12"
    arguments:
      - name: price
        data_type: float
        description: The price of the standard item
    returns:
      data_type: float
      description: The resulting xlarge price
"""

SUM_SQUARED_UDAF_PYTHON = """
class SumSquared:
  def __init__(self):
    # This aggregate state is a primitive Python data type.
    self._partial_sum = 0

  @property
  def aggregate_state(self):
    return self._partial_sum

  def accumulate(self, input_value):
    self._partial_sum += input_value

  def merge(self, other_partial_sum):
    self._partial_sum += other_partial_sum

  def finish(self):
    return self._partial_sum ** 2
"""

SUM_SQUARED_UDAF_PYTHON_YML = """
functions:
  - name: sum_squared
    description: Sums all the values, then squares the result
    config:
      type: aggregate
      entry_point: SumSquared
      runtime_version: "3.11"
    arguments:
      - name: value
        data_type: float
        description: The value to to agg (and in the end square the result)
    returns:
      data_type: float
      description: The sum of the input values, then squared
"""

SUM_SQUARED_UDAF_PYTHON_WITH_DEFAULT_ARG_YML = """
functions:
  - name: sum_squared
    description: Sums all the values, then squares the result
    config:
      type: aggregate
      entry_point: SumSquared
      runtime_version: "3.11"
    arguments:
      - name: value
        data_type: float
        description: The value to to agg (and in the end square the result)
        default_value: 1
    returns:
      data_type: float
      description: The sum of the input values, then squared
"""

BASIC_MODEL_SQL = """
SELECT 1 as id, 1 as value
UNION ALL
SELECT 2 as id, 2 as value
UNION ALL
SELECT 3 as id, 3 as value
"""

SUM_SQUARED_UDAF_SQL = """
POWER(SUM(value), 2)
"""

SUM_SQUARED_UDAF_SQL_YML = """
functions:
  - name: sum_squared
    description: Sums all the values, then squares the result
    config:
      type: aggregate
    arguments:
      - name: value
        data_type: numeric
        description: The value to to agg (and in the end square the result)
    returns:
      data_type: numeric
      description: The sum of the input values, then squared
"""

MY_UDF_WITH_DEFAULT_ARG_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: float
        description: The price of the standard item
        default_value: 100
    returns:
      data_type: float
      description: The resulting xlarge price
"""

MY_UDF_PYTHON_WITH_DEFAULT_ARG_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      entry_point: price_for_xlarge
      runtime_version: "3.12"
    arguments:
      - name: price
        data_type: float
        description: The price of the standard item
        default_value: 100
    returns:
      data_type: float
      description: The resulting xlarge price
"""

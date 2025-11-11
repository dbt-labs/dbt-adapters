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
      runtime_version: "3.11"
    arguments:
      - name: price
        data_type: float
        description: The price of the standard item
    returns:
      data_type: float
      description: The resulting xlarge price
"""

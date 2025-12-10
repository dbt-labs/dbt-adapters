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

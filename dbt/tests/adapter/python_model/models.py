basic_sql = """
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id
"""


basic_python = """
def model(dbt, _):
    dbt.config(
        materialized='table',
    )
    df =  dbt.ref("my_sql_model")
    df2 = dbt.ref("my_versioned_sql_model", v=1)
    df3 = dbt.ref("my_versioned_sql_model", version=1)
    df4 = dbt.ref("test", "my_versioned_sql_model", v=1)
    df5 = dbt.ref("test", "my_versioned_sql_model", version=1)
    df6 = dbt.source("test_source", "test_table")
    df = df.limit(2)
    return df
"""


second_sql = """
select * from {{ref('my_python_model')}}
"""


m_1 = """
{{config(materialized='table')}}
select 1 as id union all
select 2 as id union all
select 3 as id union all
select 4 as id union all
select 5 as id
"""

incremental_python = """
def model(dbt, session):
    dbt.config(materialized="incremental", unique_key='id')
    df = dbt.ref("m_1")
    if dbt.is_incremental:
        # incremental runs should only apply to part of the data
        df = df.filter(df.id > 5)
    return df
"""


PANDAS_MODEL = """
import pandas as pd

def model(dbt, session):
    dbt.config(
        materialized="table",
    )

    df = pd.DataFrame(
        {'City': ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas'],
        'Country': ['Argentina', 'Brazil', 'Chile', 'Colombia', 'Venezuela'],
        'Latitude': [-34.58, -15.78, -33.45, 4.60, 10.48],
        'Longitude': [-58.66, -47.91, -70.66, -74.08, -66.86]}
        )

    return df
"""
PYSPARK_MODEL = """
def model(dbt, session):
    dbt.config(
        materialized="table",
    )

    df = spark.createDataFrame(
        [
            ("Buenos Aires", "Argentina", -34.58, -58.66),
            ("Brasilia", "Brazil", -15.78, -47.91),
            ("Santiago", "Chile", -33.45, -70.66),
            ("Bogota", "Colombia", 4.60, -74.08),
            ("Caracas", "Venezuela", 10.48, -66.86),
        ],
        ["City", "Country", "Latitude", "Longitude"]
    )

    return df
"""

PANDAS_ON_SPARK_MODEL = """
import pyspark.pandas as ps


def model(dbt, session):
    dbt.config(
        materialized="table",
    )

    df = ps.DataFrame(
        {'City': ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas'],
        'Country': ['Argentina', 'Brazil', 'Chile', 'Colombia', 'Venezuela'],
        'Latitude': [-34.58, -15.78, -33.45, 4.60, 10.48],
        'Longitude': [-58.66, -47.91, -70.66, -74.08, -66.86]}
        )

    return df
"""

KOALAS_MODEL = """
import databricks.koalas as ks


def model(dbt, session):
    dbt.config(
        materialized="table",
    )

    df = ks.DataFrame(
        {'City': ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas'],
        'Country': ['Argentina', 'Brazil', 'Chile', 'Colombia', 'Venezuela'],
        'Latitude': [-34.58, -15.78, -33.45, 4.60, 10.48],
        'Longitude': [-58.66, -47.91, -70.66, -74.08, -66.86]}
        )

    return df
"""

from typing import Generator

import pytest


@pytest.fixture
def schema(connection, schema_name) -> Generator[str, None, None]:
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    yield schema_name
    with connection.cursor() as cursor:
        cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")


def test_columns_in_relation(connection, schema):
    table = "cross_db"
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {schema}.{table} as select 3.14 as id")
        columns = cursor.get_columns(
            schema_pattern=schema,
            tablename_pattern=table,
        )

    assert len(columns) == 1
    column = columns[0]

    (
        database_name,
        schema_name,
        table_name,
        column_name,
        type_code,
        type_name,
        precision,
        _,
        scale,
        *_,
    ) = column
    assert schema_name == schema
    assert table_name == table
    assert column_name == "id"
    assert type_code == 2
    assert type_name == "numeric"
    assert precision == 3
    assert scale == 2

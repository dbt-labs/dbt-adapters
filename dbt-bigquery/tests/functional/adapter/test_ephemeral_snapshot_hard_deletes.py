import pytest

from dbt.tests.adapter.simple_snapshot.test_ephemeral_snapshot_hard_deletes import (
    BaseSnapshotEphemeralHardDeletes,
    BaseSnapshotNewColumnTimestampStrategy,
    BaseSnapshotNewColumnSpecificCheckCols,
    BaseSnapshotNewColumnWithDeletes,
)

# BigQuery uses STRING instead of VARCHAR and TIMESTAMP is just TIMESTAMP
_bq_source_create_sql = """
create table {database}.{schema}.src_customers (
    id INT64,
    first_name STRING,
    last_name STRING,
    email STRING,
    updated_at TIMESTAMP
);
"""

_bq_source_insert_sql = """
insert into {database}.{schema}.src_customers (id, first_name, last_name, email, updated_at) values
(1, 'John', 'Doe', 'john.doe@example.com', '2023-01-01 10:00:00'),
(2, 'Jane', 'Smith', 'jane.smith@example.com', '2023-01-02 11:00:00'),
(3, 'Bob', 'Johnson', 'bob.johnson@example.com', '2023-01-03 12:00:00');
"""

_bq_source_alter_sql = """
alter table {database}.{schema}.src_customers add column dummy_column STRING;
"""

_bq_source_delete_sql = """
delete from {database}.{schema}.src_customers where id = 3;
"""


class _BigQueryFixtures:
    @pytest.fixture(scope="class")
    def source_create_sql(self):
        return _bq_source_create_sql

    @pytest.fixture(scope="class")
    def source_insert_sql(self):
        return _bq_source_insert_sql

    @pytest.fixture(scope="class")
    def source_alter_sql(self):
        return _bq_source_alter_sql

    @pytest.fixture(scope="class")
    def source_delete_sql(self):
        return _bq_source_delete_sql


class TestSnapshotEphemeralHardDeletes(_BigQueryFixtures, BaseSnapshotEphemeralHardDeletes):
    pass


class TestSnapshotNewColumnTimestampStrategy(
    _BigQueryFixtures, BaseSnapshotNewColumnTimestampStrategy
):
    pass


class TestSnapshotNewColumnSpecificCheckCols(
    _BigQueryFixtures, BaseSnapshotNewColumnSpecificCheckCols
):
    pass


class TestSnapshotNewColumnWithDeletes(_BigQueryFixtures, BaseSnapshotNewColumnWithDeletes):
    pass

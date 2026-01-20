"""
Spark-specific tests for snapshot column backfill feature.

Note: Spark requires file_format to be 'delta', 'iceberg', or 'hudi' for snapshots.
"""

import pytest

from dbt.tests.adapter.simple_snapshot.test_snapshot_backfill import (
    BaseSnapshotBackfillSingleColumn,
    BaseSnapshotBackfillMultipleColumns,
    BaseSnapshotBackfillSequential,
    BaseSnapshotBackfillAuditJson,
    BaseSnapshotBackfillCompositeKey,
    BaseSnapshotBackfillDisabled,
    BaseSnapshotBackfillNullHandling,
    BaseSnapshotBackfillBehaviorFlag,
)


# Spark-specific snapshot SQL with Delta format
SPARK_SNAPSHOT_BACKFILL_WITH_AUDIT_SQL = """
{% snapshot snapshot %}
    {{ config(
        target_schema=schema,
        strategy='timestamp',
        unique_key='id',
        updated_at='updated_at',
        file_format='delta',
        backfill_new_columns=true,
        backfill_audit_column='dbt_backfill_audit',
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""


class SparkSnapshotBackfillMixin:
    """Mixin to override snapshot SQL for Spark with Delta format."""

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SPARK_SNAPSHOT_BACKFILL_WITH_AUDIT_SQL}


class TestSnapshotBackfillSingleColumn(
    SparkSnapshotBackfillMixin, BaseSnapshotBackfillSingleColumn
):
    pass


class TestSnapshotBackfillMultipleColumns(
    SparkSnapshotBackfillMixin, BaseSnapshotBackfillMultipleColumns
):
    pass


class TestSnapshotBackfillSequential(SparkSnapshotBackfillMixin, BaseSnapshotBackfillSequential):
    pass


class TestSnapshotBackfillAuditJson(SparkSnapshotBackfillMixin, BaseSnapshotBackfillAuditJson):
    pass


class TestSnapshotBackfillCompositeKey(
    SparkSnapshotBackfillMixin, BaseSnapshotBackfillCompositeKey
):
    pass


class TestSnapshotBackfillDisabled(BaseSnapshotBackfillDisabled):
    """Uses base class since backfill is disabled anyway."""

    pass


class TestSnapshotBackfillNullHandling(
    SparkSnapshotBackfillMixin, BaseSnapshotBackfillNullHandling
):
    pass


class TestSnapshotBackfillBehaviorFlag(
    SparkSnapshotBackfillMixin, BaseSnapshotBackfillBehaviorFlag
):
    pass

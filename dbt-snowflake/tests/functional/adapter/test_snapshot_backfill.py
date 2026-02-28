"""
Snowflake-specific tests for snapshot column backfill feature.
"""

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


class TestSnapshotBackfillSingleColumn(BaseSnapshotBackfillSingleColumn):
    pass


class TestSnapshotBackfillMultipleColumns(BaseSnapshotBackfillMultipleColumns):
    pass


class TestSnapshotBackfillSequential(BaseSnapshotBackfillSequential):
    pass


class TestSnapshotBackfillAuditJson(BaseSnapshotBackfillAuditJson):
    pass


class TestSnapshotBackfillCompositeKey(BaseSnapshotBackfillCompositeKey):
    pass


class TestSnapshotBackfillDisabled(BaseSnapshotBackfillDisabled):
    pass


class TestSnapshotBackfillNullHandling(BaseSnapshotBackfillNullHandling):
    pass


class TestSnapshotBackfillBehaviorFlag(BaseSnapshotBackfillBehaviorFlag):
    pass

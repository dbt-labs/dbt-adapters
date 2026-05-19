from dbt.tests.adapter.simple_snapshot.test_ephemeral_snapshot_hard_deletes import (
    BaseSnapshotEphemeralHardDeletes,
    BaseSnapshotNewColumnTimestampStrategy,
    BaseSnapshotNewColumnSpecificCheckCols,
    BaseSnapshotNewColumnWithDeletes,
)


class TestSnapshotEphemeralHardDeletes(BaseSnapshotEphemeralHardDeletes):
    pass


class TestSnapshotNewColumnTimestampStrategy(BaseSnapshotNewColumnTimestampStrategy):
    pass


class TestSnapshotNewColumnSpecificCheckCols(BaseSnapshotNewColumnSpecificCheckCols):
    pass


class TestSnapshotNewColumnWithDeletes(BaseSnapshotNewColumnWithDeletes):
    pass

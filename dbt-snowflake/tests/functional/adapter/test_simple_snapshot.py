from dbt.tests.adapter.simple_snapshot.test_snapshot import BaseSnapshotCheck, BaseSimpleSnapshot
from dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode import (
    BaseSnapshotNewRecordTimestampMode,
)
from dbt.tests.adapter.simple_snapshot.new_record_check_mode import BaseSnapshotNewRecordCheckMode


class TestSnapshot(BaseSimpleSnapshot):
    pass


class TestSnapshotCheck(BaseSnapshotCheck):
    pass


class TestSnapshotNewRecordTimestampMode(BaseSnapshotNewRecordTimestampMode):
    pass


class TestSnapshotNewRecordCheckMode(BaseSnapshotNewRecordCheckMode):
    pass

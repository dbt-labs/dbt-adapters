from dbt.tests.adapter.simple_snapshot.new_record_check_mode import BaseSnapshotNewRecordCheckMode
from dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode import (
    BaseSnapshotNewRecordTimestampMode,
)
from dbt.tests.adapter.simple_snapshot.new_record_dbt_valid_to_current import (
    BaseSnapshotNewRecordDbtValidToCurrent,
)
from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSimpleSnapshot,
    BaseSnapshotCheck,
)


class TestSnapshot(BaseSimpleSnapshot):
    pass


class TestSnapshotCheck(BaseSnapshotCheck):
    pass


class TestSnapshotNewRecordTimestampMode(BaseSnapshotNewRecordTimestampMode):
    pass


class TestSnapshotNewRecordCheckMode(BaseSnapshotNewRecordCheckMode):
    pass


class TestSnapshotNewRecordDbtValidToCurrent(BaseSnapshotNewRecordDbtValidToCurrent):
    pass

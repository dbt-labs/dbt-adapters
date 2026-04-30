import pytest
from dbt.tests.adapter.simple_snapshot.test_ephemeral_snapshot_hard_deletes import (
    BaseSnapshotEphemeralHardDeletes,
)


@pytest.mark.skip(
    reason="Failing due to 'No location was specified for table. An S3 location must be specified' error'"
)
class TestSnapshotEphemeralHardDeletes(BaseSnapshotEphemeralHardDeletes):
    pass

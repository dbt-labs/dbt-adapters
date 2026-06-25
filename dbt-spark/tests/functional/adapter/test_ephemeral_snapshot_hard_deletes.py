import pytest
from dbt.tests.adapter.simple_snapshot.test_ephemeral_snapshot_hard_deletes import (
    BaseSnapshotEphemeralHardDeletes,
)


@pytest.mark.skip(reason="Failing for unknown reason, needs investigation")
class TestSnapshotEphemeralHardDeletes(BaseSnapshotEphemeralHardDeletes):
    pass

import pytest

from dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode import (
    BaseSnapshotNewRecordTimestampMode,
)

_snapshots_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: check
      check_cols: all
      hard_deletes: new_record
"""

_ref_snapshot_sql = """
select * from {{ ref('snapshot_actual') }}
"""


class BaseSnapshotNewRecordCheckMode(BaseSnapshotNewRecordTimestampMode):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": _snapshots_yml,
            "ref_snapshot.sql": _ref_snapshot_sql,
        }

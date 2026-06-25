"""
Athena-specific tests for snapshot column backfill feature.

Note: Athena backfill only works with Iceberg tables. Hive tables will skip backfill.
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


# Athena-specific snapshot SQL with Iceberg format
ATHENA_SNAPSHOT_BACKFILL_WITH_AUDIT_SQL = """
{% snapshot snapshot %}
    {{ config(
        target_schema=schema,
        strategy='timestamp',
        unique_key='id',
        updated_at='updated_at',
        table_type='iceberg',
        backfill_new_columns=true,
        backfill_audit_column='dbt_backfill_audit',
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""


class AthenaSnapshotBackfillMixin:
    """Mixin to override snapshot SQL for Athena with Iceberg format."""

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": ATHENA_SNAPSHOT_BACKFILL_WITH_AUDIT_SQL}


class TestSnapshotBackfillSingleColumn(
    AthenaSnapshotBackfillMixin, BaseSnapshotBackfillSingleColumn
):
    pass


class TestSnapshotBackfillMultipleColumns(
    AthenaSnapshotBackfillMixin, BaseSnapshotBackfillMultipleColumns
):
    pass


class TestSnapshotBackfillSequential(AthenaSnapshotBackfillMixin, BaseSnapshotBackfillSequential):
    pass


class TestSnapshotBackfillAuditJson(AthenaSnapshotBackfillMixin, BaseSnapshotBackfillAuditJson):
    pass


class TestSnapshotBackfillCompositeKey(
    AthenaSnapshotBackfillMixin, BaseSnapshotBackfillCompositeKey
):
    pass


class TestSnapshotBackfillDisabled(BaseSnapshotBackfillDisabled):
    """Uses base class since backfill is disabled anyway."""

    pass


class TestSnapshotBackfillNullHandling(
    AthenaSnapshotBackfillMixin, BaseSnapshotBackfillNullHandling
):
    pass


class TestSnapshotBackfillBehaviorFlag(
    AthenaSnapshotBackfillMixin, BaseSnapshotBackfillBehaviorFlag
):
    pass

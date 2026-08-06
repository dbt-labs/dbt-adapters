"""
Snapshots against a connection pointed directly at a datashare consumer database -- the same
configuration that reproduces dbt-labs/dbt-adapters#1947 and #1991 for incremental models.

Snapshots hit the identical root cause through a different door: `snapshot.sql` builds a
`*__dbt_tmp` staging relation (via `make_temp_relation`, same shape as an incremental temp
relation) and calls `adapter.get_columns_in_relation` / `adapter.get_missing_columns` on it
directly -- there is no `on_schema_change` involved, so the guard added for incremental models
(default__check_for_schema_changes) does not apply here and was never meant to.

Snapshots also fail *louder* than incremental's silent column drop: a zero-columns read on the
staging relation makes `source_columns` empty, which renders `snapshot_merge_sql`'s insert
column list empty too, producing:

    insert into "<target>" ()
    select
    from "<staging>__dbt_tmp..." as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

...a Redshift syntax error at the empty `()`. This is not a synthetic scenario -- it is the exact
failure a production snapshot hit against a real datashare consumer connection (see the customer
debug log referenced in the engagement note for issue 122399). `test_new_column_captured_by_snapshot`
and `test_inserts_are_captured_by_snapshot` below are the two cases in the shared snapshot test
base that exercise this path (a genuine new column, and a genuine new row respectively); both
would have hit that syntax error before the driver-based `get_columns_in_temp_relation` fallback.
"""

from dbt.tests.adapter.simple_snapshot.test_snapshot import BaseSimpleSnapshot, BaseSnapshotCheck

from tests.functional.adapter.datashare_consumer.fixtures import DatashareConsumerMixin


class TestSnapshotDatashareConsumer(DatashareConsumerMixin, BaseSimpleSnapshot):
    """Timestamp-strategy snapshot suite, run against a datashare consumer database."""


class TestSnapshotCheckDatashareConsumer(DatashareConsumerMixin, BaseSnapshotCheck):
    """Check-strategy snapshot suite, run against a datashare consumer database."""

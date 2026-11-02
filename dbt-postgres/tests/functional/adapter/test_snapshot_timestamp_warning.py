"""
`snapshot_get_time()` only feeds the SCD timestamp columns when hard deletes are being
handled, so the data type comparison in `check_time_data_types` is only meaningful when
`hard_deletes` resolves to `invalidate` or `new_record`.

Postgres exhibits the mismatch for free: `postgres__snapshot_get_time()` returns
`timestamp without time zone` while `updated_time` below is `timestamp with time zone`.
"""

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture


create_source_sql = """
create table {database}.{schema}.source_users (
    id INTEGER,
    updated_time TIMESTAMP WITH TIME ZONE
);
insert into {database}.{schema}.source_users (id, updated_time) values
(1, '2015-12-24 12:19:28'),
(2, '2015-10-28 16:22:15');
"""

model_users_sql = """
select * from {{ source('test_source', 'source_users') }}
"""

snapshot_sql = """
{% snapshot users_snapshot %}

select * from {{ ref('users') }}

{% endsnapshot %}
"""

source_schema_yml = """
sources:
  - name: test_source
    loader: custom
    schema: "{{ target.schema }}"
    tables:
      - name: source_users
        loaded_at_field: updated_time
"""

snapshot_schema_yml = """
snapshots:
  - name: users_snapshot
    config:
      target_schema: "{{ target.schema }}"
      strategy: timestamp
      unique_key: id
      updated_at: updated_time
"""

snapshot_schema_hard_deletes_yml = """
snapshots:
  - name: users_snapshot
    config:
      target_schema: "{{ target.schema }}"
      strategy: timestamp
      unique_key: id
      updated_at: updated_time
      hard_deletes: invalidate
"""

# Stable across the current and the reworded dbt-core `SnapshotTimestampWarning` message.
# dbt-postgres installs dbt-core from `1.latest`, so a more specific substring would be brittle.
WARNING_FRAGMENT = "Data type of snapshot table"


class BaseSnapshotTimestampWarning:
    """The `updated_at` column is timezone aware, `snapshot_get_time()` is not."""

    snapshot_schema = snapshot_schema_yml

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "users.sql": model_users_sql,
            "source_schema.yml": source_schema_yml,
            "snapshot_schema.yml": self.snapshot_schema,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshot_sql}

    def snapshot_log_output(self, project):
        project.run_sql(create_source_sql)
        run_dbt(["run"])
        results, log_output = run_dbt_and_capture(["snapshot"])
        assert len(results) == 1
        return log_output


class TestSnapshotTimestampWarningHardDeletes(BaseSnapshotTimestampWarning):
    """`hard_deletes: invalidate` writes `snapshot_get_time()`, so the warning still fires."""

    snapshot_schema = snapshot_schema_hard_deletes_yml

    def test_warning_fires(self, project):
        assert WARNING_FRAGMENT in self.snapshot_log_output(project)


class TestSnapshotTimestampWarningIgnoreHardDeletes(BaseSnapshotTimestampWarning):
    """Default `hard_deletes` is `ignore`, so `snapshot_get_time()` is never written."""

    def test_warning_suppressed(self, project):
        assert WARNING_FRAGMENT not in self.snapshot_log_output(project)

import pytest

from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.python_model.test_python_model import BasePythonIncrementalTests
from dbt.tests.util import run_dbt, run_dbt_and_capture

FLAG = "snowflake_fail_on_unexpected_existing_relation"

# Pre-hooks run after the materialization looked the target up in dbt's relation cache, so
# creating {{ this }} here reproduces "absent from the listing, present in Snowflake".
OUT_OF_BAND_HOOKS = """
      pre_hook:
        - "create transient table if not exists {{ this }} (id integer)"
        - "insert into {{ this }} (id) values (999)"
"""

INCREMENTAL_SQL = """
{{ config(materialized='incremental', unique_key='id') }}
select 1 as id
"""

TABLE_SQL = """
{{ config(materialized='table') }}
select 1 as id
"""

INCREMENTAL_PY = """
def model(dbt, session):
    dbt.config(materialized="incremental", unique_key="id")
    return session.sql("select 1 as id")
"""

SNAPSHOT_SQL = """
{% snapshot snap %}
{{ config(
    target_schema=schema,
    unique_key='id',
    strategy='check',
    check_cols='all',
    pre_hook=[
        "create transient table if not exists {{ this }} (id integer)",
        "insert into {{ this }} (id) values (999)",
    ],
) }}
select 1 as id
{% endsnapshot %}
"""


SNAPSHOT_PLAIN_SQL = """
{% snapshot snap %}
{{ config(target_schema=schema, unique_key='id', strategy='check', check_cols='all') }}
select 1 as id
{% endsnapshot %}
"""

# COPY GRANTS / COPY TAGS are only valid with OR REPLACE, so the plain-create path must omit them.
COPY_OPTIONS = {"+copy_grants": True, "+copy_tags": True}


def _properties_with_out_of_band_hooks(*model_names):
    entries = "".join(f"  - name: {name}\n    config:{OUT_OF_BAND_HOOKS}" for name in model_names)
    return "models:\n" + entries


def _ids(project, name):
    rows = project.run_sql(
        f"select id from {project.database}.{project.test_schema}.{name}", fetch="all"
    )
    return sorted(row[0] for row in rows)


class _FlagOn:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {FLAG: True}}


class _FlagOff:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {FLAG: False}}


class TestIncrementalUnexpectedExistingRelationFails(_FlagOn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incr.sql": INCREMENTAL_SQL,
            "properties.yml": _properties_with_out_of_band_hooks("incr"),
        }

    def test_run_fails_and_keeps_existing_rows(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "already exists" in results[0].message.lower()
        assert _ids(project, "incr") == [999]


class TestIncrementalUnexpectedExistingRelationFailsByDefault:
    """No flag set: the protective behavior is the default."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incr.sql": INCREMENTAL_SQL,
            "properties.yml": _properties_with_out_of_band_hooks("incr"),
        }

    def test_run_fails_and_keeps_existing_rows(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "already exists" in results[0].message.lower()
        assert _ids(project, "incr") == [999]


class TestIncrementalUnexpectedExistingRelationLegacyReplaces(_FlagOff):
    """Documents the behavior the flag fixes: the existing rows are silently replaced."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incr.sql": INCREMENTAL_SQL,
            "properties.yml": _properties_with_out_of_band_hooks("incr"),
        }

    def test_run_replaces_existing_rows(self, project):
        run_dbt(["run"])
        assert _ids(project, "incr") == [1]


class TestIncrementalFullRefreshStillReplaces(_FlagOn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incr.sql": INCREMENTAL_SQL,
            "properties.yml": _properties_with_out_of_band_hooks("incr"),
        }

    def test_full_refresh_replaces(self, project):
        run_dbt(["run", "--full-refresh"])
        assert _ids(project, "incr") == [1]


class TestIncrementalLifecycleWithFlag(_FlagOn):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incr.sql": INCREMENTAL_SQL}

    def test_first_run_creates_then_merges(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert "create transient table" in logs.lower()
        assert "create or replace transient table" not in logs.lower()
        run_dbt(["run"])
        assert _ids(project, "incr") == [1]


class TestTableModelStillReplaces(_FlagOn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tbl.sql": TABLE_SQL,
            "properties.yml": _properties_with_out_of_band_hooks("tbl"),
        }

    def test_run_replaces(self, project):
        run_dbt(["run"])
        assert _ids(project, "tbl") == [1]


class TestSnapshotUnexpectedExistingRelationFails(_FlagOn):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.sql": SNAPSHOT_SQL}

    def test_snapshot_fails_and_keeps_existing_rows(self, project):
        results = run_dbt(["snapshot"], expect_pass=False)
        assert "already exists" in results[0].message.lower()
        assert _ids(project, "snap") == [999]


class TestSnapshotUnexpectedExistingRelationLegacyReplaces(_FlagOff):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.sql": SNAPSHOT_SQL}

    def test_snapshot_replaces_existing_rows(self, project):
        run_dbt(["snapshot"])
        assert _ids(project, "snap") == [1]


class TestPythonIncrementalUnexpectedExistingRelationFails(_FlagOn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "py_incr.py": INCREMENTAL_PY,
            "properties.yml": _properties_with_out_of_band_hooks("py_incr"),
        }

    def test_run_fails_and_keeps_existing_rows(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "already exists" in results[0].message.lower()
        assert _ids(project, "py_incr") == [999]


class TestIncrementalLifecycleWithCopyOptions:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": COPY_OPTIONS}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incr.sql": INCREMENTAL_SQL}

    def test_first_run_merge_and_full_refresh_succeed(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        run_dbt(["run", "--full-refresh"])
        assert _ids(project, "incr") == [1]


class TestSnapshotFirstRunWithCopyOptions:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"snapshots": COPY_OPTIONS}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.sql": SNAPSHOT_PLAIN_SQL}

    def test_first_and_second_snapshot_succeed(self, project):
        run_dbt(["snapshot"])
        run_dbt(["snapshot"])
        assert _ids(project, "snap") == [1]


class TestIncrementalUnexpectedExistingRelationWithCopyOptions:
    """The failure must be the 'already exists' one, not a COPY GRANTS syntax error."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": COPY_OPTIONS}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incr.sql": INCREMENTAL_SQL,
            "properties.yml": _properties_with_out_of_band_hooks("incr"),
        }

    def test_run_fails_with_already_exists_and_keeps_rows(self, project):
        results = run_dbt(["run"], expect_pass=False)
        message = results[0].message.lower()
        assert "already exists" in message
        assert "copy grants" not in message
        assert _ids(project, "incr") == [999]


# Upstream suites re-run with the flag on: normal lifecycles must be unaffected.
# Each base class defines project_config_update; merge into it, don't replace it.
class TestBaseIncrementalWithFlag(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental", "flags": {FLAG: True}}


class TestSnapshotCheckColsWithFlag(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "snapshot_strategy_check_cols", "flags": {FLAG: True}}


class TestSnapshotTimestampWithFlag(BaseSnapshotTimestamp):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "snapshot_strategy_timestamp", "flags": {FLAG: True}}


class TestPythonIncrementalWithFlag(BasePythonIncrementalTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "merge"}, "flags": {FLAG: True}}

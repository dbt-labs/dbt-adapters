"""
Functional tests for the CREATE OR ALTER dynamic table path (query evolution).

Behavior flag `snowflake_dynamic_table_create_or_alter` makes any native (info schema) dynamic
table sync its definition in place on every run, so a SQL edit deploys without --full-refresh
(matching CTAS ergonomics). For INCREMENTAL/AUTO refresh modes a definition edit reinitializes the
table (incremental state discarded). Iceberg dynamic tables are excluded (this adapter does not
apply CREATE OR ALTER to them yet). These require a live Snowflake connection.

Test flow (shared by every test in this module):
  1. ARRANGE -- the `setup` fixture builds a fresh dynamic table (`seed` + `run`); the
     behavior flag is turned on via `project_config_update` on the test class.
  2. ACT -- the test edits the model (SQL change, config change, or a refresh-mode/transient
     variant), then runs `dbt run` WITHOUT --full-refresh -- this is what exercises the
     COA path. Some tests use `run_dbt_and_capture` to inspect the run log.
  3. ASSERT -- read the live table back with `describe_dynamic_table` (SHOW output) and
     assert the definition/attribute changed as expected. A no-op skip is detected by the
     "No configuration changes were identified" log line (present == skipped).
  4. TEARDOWN -- the `setup` fixture drops the schema so each test starts clean.

Each class overrides only what differs: its `models` fixture (the starting model) and,
where relevant, `project_config_update` (flag on/off, on_configuration_change).
"""

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

from tests.functional.utils import describe_dynamic_table, query_transient_status, update_model


SEED = """
id,value
1,alice
2,bob
3,carol
""".strip()

# scheduler='disable' + FULL => dbt-managed refresh; the COA path applies here.
DT_DISABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    refresh_mode='FULL',
    scheduler='disable',
) }}
select id, value from {{ ref('my_seed') }}
"""

# COA-compatible edit: add a column at the end.
DT_DISABLE_EVOLVED = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    refresh_mode='FULL',
    scheduler='disable',
) }}
select id, value, id * 10 as id_x10 from {{ ref('my_seed') }}
"""


# Positive deploy matrix -- (refresh_mode, scheduler, target_lag). With the flag on, a native DT
# deploys a SQL-only edit in place via COA for every combination; scheduler='enable' needs target_lag.
_DEPLOY_MATRIX = [
    ("FULL", "disable", None),
    ("FULL", "enable", "1 hour"),
    ("INCREMENTAL", "disable", None),
    ("INCREMENTAL", "enable", "1 hour"),
    ("AUTO", "disable", None),
    ("AUTO", "enable", "1 hour"),
]
_DEPLOY_IDS = [f"{rm.lower()}-{sched}" for rm, sched, _ in _DEPLOY_MATRIX]


def _dt_model(refresh_mode, scheduler, target_lag, evolved=False):
    """A native dynamic-table model for the deploy matrix; evolved=True adds a trailing column."""
    cols = "id, value, id * 10 as id_x10" if evolved else "id, value"
    lag = f", target_lag='{target_lag}'" if target_lag else ""
    return (
        "{{ config(materialized='dynamic_table', snowflake_warehouse='DBT_TESTING', "
        f"refresh_mode='{refresh_mode}', scheduler='{scheduler}'{lag}) }}}}\n"
        f"select {cols} from {{{{ ref('my_seed') }}}}"
    )


class TestCreateOrAlterDeployMatrix:
    """Positive matrix: with the flag on, a native DT deploys a SQL-only edit in place via COA for
    every refresh_mode x scheduler combination -- the surface this feature opens up. created_on
    stability proves the sync was in place (COA), not a drop/recreate (replace). For
    scheduler='enable' dbt issues no immediate refresh, so we assert the definition (SHOW text)
    changed; data lags to the next scheduled refresh. INCREMENTAL/AUTO reinitialize their data on
    the edit (a documented Snowflake-side effect), but the object -- and its created_on -- persist.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        # placeholder; each parametrized case rebuilds dt_coa for its own refresh_mode/scheduler
        yield {"dt_coa.sql": DT_DISABLE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"snowflake_dynamic_table_create_or_alter": True}}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    @pytest.mark.parametrize(
        "refresh_mode, scheduler, target_lag", _DEPLOY_MATRIX, ids=_DEPLOY_IDS
    )
    def test_sql_edit_deploys_in_place(self, project, refresh_mode, scheduler, target_lag):
        # build a fresh baseline DT for this cell
        update_model(project, "dt_coa", _dt_model(refresh_mode, scheduler, target_lag))
        run_dbt(["run", "--full-refresh"])
        before = describe_dynamic_table(project, "dt_coa")
        assert "id_x10" not in (before.query or "").lower()
        before_created = _created_on(project)

        # SQL-only edit + run WITHOUT --full-refresh -> exercises the COA path
        update_model(
            project, "dt_coa", _dt_model(refresh_mode, scheduler, target_lag, evolved=True)
        )
        _, logs = run_dbt_and_capture(["run"])

        # definition deployed (not a no-op skip), for every refresh_mode x scheduler
        after = describe_dynamic_table(project, "dt_coa")
        assert "id_x10" in (after.query or "").lower()
        assert "No configuration changes were identified" not in logs
        # synced in place via CREATE OR ALTER, not dropped/recreated (proves COA, not replace)
        assert _created_on(project) == before_created


DT_TRANSIENT = """
{{ config(materialized='dynamic_table', snowflake_warehouse='DBT_TESTING',
          refresh_mode='FULL', scheduler='disable', transient=true) }}
select id, value from {{ ref('my_seed') }}
"""
DT_NON_TRANSIENT = """
{{ config(materialized='dynamic_table', snowflake_warehouse='DBT_TESTING',
          refresh_mode='FULL', scheduler='disable', transient=false) }}
select id, value from {{ ref('my_seed') }}
"""

# COA-incompatible edit: column reorder (Snowflake rejects it in CREATE OR ALTER).
DT_REORDERED = """
{{ config(materialized='dynamic_table', snowflake_warehouse='DBT_TESTING',
          refresh_mode='FULL', scheduler='disable') }}
select value, id from {{ ref('my_seed') }}
"""


class TestCreateOrAlterTransientFlipRebuilds:
    """transient can't be changed via COA -> the requires_full_refresh veto routes to CREATE OR
    REPLACE, so the run succeeds (not a COA error) and the transient status flips."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_TRANSIENT}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"snowflake_dynamic_table_create_or_alter": True}}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_transient_flip_routes_to_replace(self, project):
        assert query_transient_status(project, "dt_coa") is True
        update_model(project, "dt_coa", DT_NON_TRANSIENT)
        results = run_dbt(["run"])  # must succeed (veto -> CREATE OR REPLACE), not error on COA
        assert len(results) == 1
        assert query_transient_status(project, "dt_coa") is False


class TestCreateOrAlterOnConfigurationChangeFail:
    """on_configuration_change='fail' still fails on a detected config change under the COA path."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_DISABLE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"snowflake_dynamic_table_create_or_alter": True},
            "models": {"on_configuration_change": "fail"},
        }

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_config_change_fails_fast(self, project):
        # change the warehouse -> tracked config change -> on_configuration_change='fail' -> error
        changed = DT_DISABLE.replace("'DBT_TESTING'", "'DBT_TESTING_ALT'")
        update_model(project, "dt_coa", changed)
        _, logs = run_dbt_and_capture(["run"], expect_pass=False)
        # the failure is the fail-fast config-change path, not some unrelated error
        assert "Configuration changes were identified" in logs
        assert "`on_configuration_change` was set to `fail`" in logs


class TestCreateOrAlterOnConfigurationChangeContinue:
    """on_configuration_change='continue' under the COA path: a run carrying a tracked config change
    warns and no-ops. Known limitation (documented): a coincident SQL edit is skipped along with the
    config change -- the changeset can't see the query, so 'continue' drops both. This test pins that
    behavior so a future change that silently starts deploying the edit is caught."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_DISABLE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"snowflake_dynamic_table_create_or_alter": True},
            "models": {"on_configuration_change": "continue"},
        }

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_continue_skips_both_config_and_sql_edit(self, project):
        # change the warehouse (tracked config change) AND edit the SQL in the same run
        changed = DT_DISABLE_EVOLVED.replace("'DBT_TESTING'", "'DBT_TESTING_ALT'")
        update_model(project, "dt_coa", changed)
        results, logs = run_dbt_and_capture(["run"])  # succeeds (no-op), does not error
        assert len(results) == 1
        assert "`on_configuration_change` was set to `continue`" in logs
        # documented limitation: the SQL edit is dropped along with the config change
        after = describe_dynamic_table(project, "dt_coa")
        assert "id_x10" not in (after.query or "").lower()


class TestCreateOrAlterIncompatibleSchemaChangeErrors:
    """A schema change COA cannot express (column reorder) surfaces Snowflake's error, rather than
    silently producing a wrong result. Manual fallback is --full-refresh."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_DISABLE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"snowflake_dynamic_table_create_or_alter": True}}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_reorder_errors_then_full_refresh_recovers(self, project):
        update_model(project, "dt_coa", DT_REORDERED)
        run_dbt(["run"], expect_pass=False)  # COA rejects the reorder
        # the documented workaround rebuilds cleanly
        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 1


DT_INCREMENTAL = """
{{ config(materialized='dynamic_table', snowflake_warehouse='DBT_TESTING',
          refresh_mode='INCREMENTAL', scheduler='disable') }}
select id, value from {{ ref('my_seed') }}
"""


class TestCreateOrAlterRefreshModeChangeRebuilds:
    """A detected refresh_mode change (INCREMENTAL -> FULL) is flagged requires_full_refresh, so the
    COA entry macro routes it to CREATE OR REPLACE rather than an in-place CREATE OR ALTER. The run
    succeeds and the object is rebuilt (created_on changes) -- the discriminator proving the replace
    route, not COA. (Only the refresh_mode differs between the two models.)"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_INCREMENTAL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"snowflake_dynamic_table_create_or_alter": True}}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_refresh_mode_change_routes_to_replace(self, project):
        before_created = _created_on(project)
        update_model(project, "dt_coa", DT_DISABLE)  # INCREMENTAL -> FULL, nothing else differs
        results = run_dbt(["run"])  # must succeed via the requires_full_refresh -> replace route
        assert len(results) == 1
        after = describe_dynamic_table(project, "dt_coa")
        assert "FULL" in str(after.refresh_mode).upper()
        # rebuilt (new object) -> created_on changed: proves replace, not in-place COA
        assert _created_on(project) != before_created


class TestCreateOrAlterRefreshModeToAutoApplies:
    """Reviewer question: does an undetected refresh_mode change to AUTO break COA? It does not.
    dbt's changeset ignores changes *to* AUTO, so the COA path re-declares refresh_mode=AUTO and
    Snowflake applies it in place (AUTO resolves to a concrete mode, e.g. ADAPTIVE) -- the run
    succeeds, no error, no --full-refresh needed. (Contrast the transient flip, which Snowflake
    rejects: TestCreateOrAlterTransientDriftErrors.)"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_INCREMENTAL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"snowflake_dynamic_table_create_or_alter": True}}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_refresh_mode_to_auto_applies_via_coa(self, project):
        assert "INCREMENTAL" in str(describe_dynamic_table(project, "dt_coa").refresh_mode).upper()
        # change refresh_mode to AUTO -- undetected by the changeset (guarded on != AUTO), so it
        # takes the COA path rather than the requires_full_refresh -> replace route
        update_model(project, "dt_coa", _dt_model("AUTO", "disable", None))
        results = run_dbt(["run"])  # must succeed in place, no error, no --full-refresh
        assert len(results) == 1
        after = describe_dynamic_table(project, "dt_coa")
        # AUTO resolves to a concrete mode on readback (ADAPTIVE), never left as INCREMENTAL
        assert str(after.refresh_mode).upper() in ("AUTO", "ADAPTIVE")


class TestCreateOrAlterPreservesTransient:
    """Review comment: a transient=true DT with a SQL-only edit (transient unchanged) deploys via
    COA and stays transient. The requires_full_refresh veto only fires on a transient *change*, so
    a query-only edit must NOT route to CREATE OR REPLACE -- the DT keeps transient=YES in place.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_TRANSIENT}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"snowflake_dynamic_table_create_or_alter": True}}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_transient_preserved_across_coa_edit(self, project):
        assert query_transient_status(project, "dt_coa") is True
        before = _created_on(project)
        # SQL-only edit, transient unchanged: COA syncs in place, no replace, still transient
        transient_evolved = DT_TRANSIENT.replace(
            "select id, value", "select id, value, id * 10 as id_x10"
        )
        update_model(project, "dt_coa", transient_evolved)
        _, logs = run_dbt_and_capture(["run"])
        after = describe_dynamic_table(project, "dt_coa")
        assert "id_x10" in (after.query or "").lower()
        assert query_transient_status(project, "dt_coa") is True
        assert "No configuration changes were identified" not in logs
        # synced in place, not rebuilt
        assert _created_on(project) == before


class TestCreateOrAlterTransientDriftErrors:
    """Known limitation (reviewer-requested): when `transient` is unset in config, dbt does not
    compare it, so a divergence between the effective transient default and the live table goes
    undetected -- the COA path re-declares transient and Snowflake rejects the in-place flip
    (error 001521). Here a DT is created transient (explicit `transient=true`), then `transient` is
    removed from config alongside a SQL edit; with the transient-default flag off, COA tries to make
    it non-transient and errors. --full-refresh recovers by rebuilding. (Same mechanism as flipping
    snowflake_default_transient_dynamic_tables between runs -- the case raised in review.)"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_TRANSIENT}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # COA on; snowflake_default_transient_dynamic_tables left OFF, so an unset `transient`
        # resolves to non-transient -- the divergence that triggers the flip.
        return {"flags": {"snowflake_dynamic_table_create_or_alter": True}}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_transient_drift_errors_then_full_refresh_recovers(self, project):
        assert query_transient_status(project, "dt_coa") is True
        # remove `transient` from config (now unset) + edit the SQL; default flag off -> COA emits
        # a non-transient CREATE OR ALTER against a transient table -> Snowflake 001521
        update_model(project, "dt_coa", DT_DISABLE_EVOLVED)
        run_dbt(["run"], expect_pass=False)
        # documented recovery: full rebuild lands it as non-transient
        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 1
        assert query_transient_status(project, "dt_coa") is False


# Iceberg dynamic table -- deliberately excluded from the COA path by the gate (INFO_SCHEMA only).
# Snowflake supports CREATE OR ALTER DYNAMIC ICEBERG TABLE (since 2026-08-13), but this adapter
# does not implement that DDL path yet (tracked as follow-up), so Iceberg keeps create-or-replace.
DT_ICEBERG_DISABLE = """
{{ config(materialized='dynamic_table', snowflake_warehouse='DBT_TESTING',
          refresh_mode='FULL', scheduler='disable',
          table_format='iceberg', external_volume='s3_iceberg_snow',
          base_location_subpath='coa_iceberg_guard') }}
select id, value from {{ ref('my_seed') }}
"""
DT_ICEBERG_DISABLE_EVOLVED = """
{{ config(materialized='dynamic_table', snowflake_warehouse='DBT_TESTING',
          refresh_mode='FULL', scheduler='disable',
          table_format='iceberg', external_volume='s3_iceberg_snow',
          base_location_subpath='coa_iceberg_guard') }}
select id, value, id * 10 as id_x10 from {{ ref('my_seed') }}
"""


class TestCreateOrAlterExcludesIceberg:
    """The COA path is gated to the native (INFO_SCHEMA) catalog, so an Iceberg dynamic table falls
    through to the legacy path and a SQL-only edit still no-ops. This is a deliberate scoping
    decision, not a platform limit: Snowflake supports CREATE OR ALTER DYNAMIC ICEBERG TABLE (since
    2026-08-13), but this adapter does not implement that DDL path yet (tracked as follow-up).

    Requires the CI Iceberg external volume (`s3_iceberg_snow`), like the other TestIceberg* cases.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_ICEBERG_DISABLE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"snowflake_dynamic_table_create_or_alter": True}}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_iceberg_sql_edit_is_noop_even_with_flag(self, project):
        update_model(project, "dt_coa", DT_ICEBERG_DISABLE_EVOLVED)
        _, logs = run_dbt_and_capture(["run"])
        after = describe_dynamic_table(project, "dt_coa")
        # not evolved: Iceberg is excluded from the COA path in this adapter (see class docstring)
        assert "id_x10" not in (after.query or "").lower()
        assert "No configuration changes were identified" in logs


class TestCreateOrAlterFlagOffIsNoop:
    """Control: with the flag off, a SQL-only edit still no-ops (documented gap)."""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_DISABLE}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_sql_only_edit_is_noop_without_flag(self, project):
        update_model(project, "dt_coa", DT_DISABLE_EVOLVED)
        _, logs = run_dbt_and_capture(["run"])
        after = describe_dynamic_table(project, "dt_coa")
        # unchanged: the old query is still live
        assert "id_x10" not in (after.query or "").lower()


def _created_on(project):
    """`created_on` (first column of SHOW DYNAMIC TABLES) changes only if the object is
    dropped/recreated -- a stable value across a run proves it was synced in place, not rebuilt."""
    rows = project.run_sql(
        f"show dynamic tables like 'dt_coa' in schema {project.database}.{project.test_schema}",
        fetch="all",
    )
    return rows[0][0] if rows else None


class TestCreateOrAlterNoChangeIsNoop:
    """With the flag on, a `dbt run` with NO model change is an idempotent no-op: the dynamic table
    is synced in place via CREATE OR ALTER, not rebuilt -- so the object is preserved (created_on
    unchanged) and the run succeeds. Covers the "no-op when nothing changes" behavior in issue #1825.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"dt_coa.sql": DT_DISABLE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"snowflake_dynamic_table_create_or_alter": True}}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_no_change_run_is_idempotent_noop(self, project):
        before = _created_on(project)
        assert before is not None
        # identical model, no --full-refresh: COA syncs in place -> no rebuild, no error
        results = run_dbt(["run"])
        assert len(results) == 1
        after = _created_on(project)
        assert (
            after == before
        ), f"object was rebuilt (created_on {before} -> {after}), expected no-op"

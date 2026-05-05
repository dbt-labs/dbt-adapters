"""Functional tests for the `drop_without_cascade` profile credential and
model-level config.

These exercise the rendered SQL by inspecting dbt's debug logs after a
re-run that forces a DROP. The first run creates the relation; the
second run drops + recreates it, surfacing the DROP statement in logs.
"""

import os

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture


_VIEW_MODEL = """
{{ config(materialized='view') }}
select 1 as id
"""

_VIEW_MODEL_OPT_IN = """
{{ config(materialized='view', drop_without_cascade=true) }}
select 1 as id
"""

_VIEW_MODEL_OPT_OUT = """
{{ config(materialized='view', drop_without_cascade=false) }}
select 1 as id
"""


def _drop_lines(logs: str, relation_name: str) -> list[str]:
    """Return log lines containing a DROP statement for the given relation."""
    matches = []
    for line in logs.splitlines():
        lowered = line.lower()
        if "drop view if exists" in lowered and relation_name in lowered:
            matches.append(line)
    return matches


class TestDropWithCascadeByDefault:
    """Default profile + default model config → DROP must include CASCADE."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_view.sql": _VIEW_MODEL}

    def test_cascade_present_by_default(self, project):
        run_dbt(["run"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        drop_lines = _drop_lines(logs, "my_view")
        assert drop_lines, f"expected at least one DROP VIEW for my_view; logs:\n{logs}"
        assert all("cascade" in line.lower() for line in drop_lines), drop_lines


class TestDropWithoutCascadeProfileLevel:
    """`drop_without_cascade=True` in the profile → DROP must omit CASCADE."""

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "redshift",
            "host": os.getenv("REDSHIFT_TEST_HOST"),
            "port": int(os.getenv("REDSHIFT_TEST_PORT", "5439")),
            "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
            "user": os.getenv("REDSHIFT_TEST_USER"),
            "pass": os.getenv("REDSHIFT_TEST_PASS"),
            "region": os.getenv("REDSHIFT_TEST_REGION"),
            "threads": 1,
            "retries": 6,
            "drop_without_cascade": True,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_view.sql": _VIEW_MODEL}

    def test_cascade_absent_when_profile_opts_in(self, project):
        run_dbt(["run"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        drop_lines = _drop_lines(logs, "my_view")
        assert drop_lines, f"expected at least one DROP VIEW for my_view; logs:\n{logs}"
        for line in drop_lines:
            assert "cascade" not in line.lower(), line


class TestDropWithoutCascadeModelLevel:
    """`drop_without_cascade=true` model config opts in for that model only."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_default.sql": _VIEW_MODEL,
            "view_opt_in.sql": _VIEW_MODEL_OPT_IN,
        }

    def test_model_config_overrides_profile_default(self, project):
        run_dbt(["run"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        # Default model still gets CASCADE
        default_drops = _drop_lines(logs, "view_default")
        assert default_drops, f"expected DROP VIEW for view_default; logs:\n{logs}"
        assert all("cascade" in line.lower() for line in default_drops), default_drops

        # Opt-in model omits CASCADE
        opt_in_drops = _drop_lines(logs, "view_opt_in")
        assert opt_in_drops, f"expected DROP VIEW for view_opt_in; logs:\n{logs}"
        for line in opt_in_drops:
            assert "cascade" not in line.lower(), line


class TestDropWithoutCascadeModelOverridesProfile:
    """Profile is opt-in; per-model `drop_without_cascade=false` keeps CASCADE."""

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "redshift",
            "host": os.getenv("REDSHIFT_TEST_HOST"),
            "port": int(os.getenv("REDSHIFT_TEST_PORT", "5439")),
            "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
            "user": os.getenv("REDSHIFT_TEST_USER"),
            "pass": os.getenv("REDSHIFT_TEST_PASS"),
            "region": os.getenv("REDSHIFT_TEST_REGION"),
            "threads": 1,
            "retries": 6,
            "drop_without_cascade": True,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_inherits.sql": _VIEW_MODEL,
            "view_opt_out.sql": _VIEW_MODEL_OPT_OUT,
        }

    def test_model_false_overrides_profile_true(self, project):
        run_dbt(["run"])
        _, logs = run_dbt_and_capture(["--debug", "run"])

        # Inherits profile → no CASCADE
        inherit_drops = _drop_lines(logs, "view_inherits")
        assert inherit_drops, f"expected DROP VIEW for view_inherits; logs:\n{logs}"
        for line in inherit_drops:
            assert "cascade" not in line.lower(), line

        # Explicit opt-out → CASCADE present
        opt_out_drops = _drop_lines(logs, "view_opt_out")
        assert opt_out_drops, f"expected DROP VIEW for view_opt_out; logs:\n{logs}"
        assert all("cascade" in line.lower() for line in opt_out_drops), opt_out_drops

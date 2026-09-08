import pytest

from dbt.tests.util import run_dbt_and_capture

# CORE-903: `+transient` was accepted as a seed config but silently ignored —
# seeds always compiled to a plain `create table`, never `create transient table`.

_SEED_DEFAULT = "id\n1\n2\n"


class TestSeedTransientDefault:
    """Seeds default to transient, matching the table materialization's default."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": _SEED_DEFAULT}

    def test_seed_creates_transient_table(self, project):
        _, logs = run_dbt_and_capture(["--debug", "seed"])
        assert "create transient table" in logs.lower()


class TestSeedTransientFalse:
    """Explicit `transient: false` must produce a plain permanent table."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": _SEED_DEFAULT}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+transient": False}}

    def test_seed_creates_permanent_table(self, project):
        _, logs = run_dbt_and_capture(["--debug", "seed"])
        assert "create transient table" not in logs.lower()
        assert "create table" in logs.lower()

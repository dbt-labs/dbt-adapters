import pytest

from dbt.tests.adapter.basic.files import seeds_base_csv
from dbt.tests.util import run_dbt


class TestAthenaSeedNoRename:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": seeds_base_csv}

    def test_seed_runs_twice(self, project):
        first = run_dbt(["seed"])
        second = run_dbt(["seed"])
        assert len(first) == 1
        assert len(second) == 1

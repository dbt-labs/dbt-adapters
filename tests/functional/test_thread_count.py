from dbt.tests.util import run_dbt
import pytest


class TestThreadCount:
    @pytest.fixture(scope="class")
    def models(self):
        sql = "with x as (select pg_sleep(1)) select 1"
        independent_models = {f"do_nothing_{num}.sql": sql for num in range(1, 21)}
        return independent_models

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2}

    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        return {"threads": 2}

    def test_threading_8x(self, project):
        results = run_dbt(args=["run", "--threads", "16"])
        assert len(results), 20

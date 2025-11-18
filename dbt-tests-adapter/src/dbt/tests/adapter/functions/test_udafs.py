import pytest

from dbt.tests.adapter.functions import files
from dbt.tests.util import run_dbt


class UDAFBase:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_model.sql": files.BASIC_MODEL_SQL,
        }

    @pytest.fixture(scope="class")
    def functions(self):
        raise NotImplementedError("Test subclass must implement functions")

    def test_udaf(self, project):
        # build the function
        result = run_dbt(["build", "--debug"])

        # try using the aggregate function
        result = run_dbt(
            [
                "show",
                "--inline",
                "SELECT {{ function('sum_squared') }}(value) FROM {{ ref('basic_model') }}",
            ]
        )
        assert len(result.results) == 1
        # 1 + 2 + 3 = 6, then 6^2 = 36
        assert result.results[0].agate_table.rows[0].values()[0] == 36.0


class BasicPythonUDAF(UDAFBase):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "sum_squared.py": files.SUM_SQUARED_UDAF_PYTHON,
            "sum_squared.yml": files.SUM_SQUARED_UDAF_PYTHON_YML,
        }


class BasicSQLUDAF(UDAFBase):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "sum_squared.sql": files.SUM_SQUARED_UDAF_SQL,
            "sum_squared.yml": files.SUM_SQUARED_UDAF_SQL_YML,
        }

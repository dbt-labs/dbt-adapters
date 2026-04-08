import pytest

from dbt.tests.adapter.functions.test_udafs import BasicPythonUDAF, PythonUDAFDefaultArgSupport


class TestSnowflakePythonUDAF(BasicPythonUDAF):
    pass


class TestSnowflakePythonUDAFDefaultArgSupport(PythonUDAFDefaultArgSupport):
    expect_default_arg_support = True


class TestSnowflakePythonUDAFVolatilitySupport(BasicPythonUDAF):
    def check_function_volatility(self, sql: str):
        assert "VOLATILE" in sql

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "non-deterministic"},
        }

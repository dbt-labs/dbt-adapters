import pytest
from dbt.tests.adapter.functions.test_udfs import (
    UDFsBasic,
    DeterministicUDF,
    StableUDF,
    NonDeterministicUDF,
)
from tests.functional.functions import files


class TestBigqueryUDFs(UDFsBasic):

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": files.MY_UDF_SQL,
            "price_for_xlarge.yml": files.MY_UDF_YML,
        }


class TestBigqueryDeterministicUDFs(DeterministicUDF):
    def check_function_volatility(self, sql: str):
        assert "VOLATILE" not in sql
        assert "STABLE" not in sql
        assert "IMMUTABLE" not in sql


class TestBigqueryStableUDFs(StableUDF):
    def check_function_volatility(self, sql: str):
        assert "VOLATILE" not in sql
        assert "STABLE" not in sql
        assert "IMMUTABLE" not in sql


class TestBigqueryNonDeterministicUDFs(NonDeterministicUDF):
    def check_function_volatility(self, sql: str):
        assert "VOLATILE" not in sql
        assert "STABLE" not in sql
        assert "IMMUTABLE" not in sql

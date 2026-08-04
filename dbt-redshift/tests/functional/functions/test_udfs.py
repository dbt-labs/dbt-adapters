import pytest
from dbt.tests.adapter.functions.files import MY_UDF_YML, MY_UDF_WITH_DEFAULT_ARG_YML
from dbt.tests.adapter.functions.test_udfs import (
    UDFsBasic,
    CanFindScalarFunctionRelation,
    DeterministicUDF,
    StableUDF,
    NonDeterministicUDF,
    ErrorForUnsupportedType,
    PythonUDFNotSupported,
    SqlUDFDefaultArgSupport,
)
from tests.functional.functions.files import MY_UDF_SQL


class TestRedshiftUDFs(UDFsBasic):

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_YML,
        }

    def check_function_volatility(self, sql: str):
        # in redshift, if no volatility is set, we template in VOLATILE
        assert "VOLATILE" in sql


class TestRedshiftDeterministicUDFs(DeterministicUDF):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_YML,
        }


class TestRedshiftStableUDFs(StableUDF):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_YML,
        }


class TestRedshiftNonDeterministicUDFs(NonDeterministicUDF):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_YML,
        }


class TestRedshiftErrorForUnsupportedType(ErrorForUnsupportedType):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_YML,
        }


class TestRedshiftPythonUDFNotSupported(PythonUDFNotSupported):
    pass


class TestRedshiftDefaultArgsSupportSQLUDFs(SqlUDFDefaultArgSupport):
    expect_default_arg_support = False

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_WITH_DEFAULT_ARG_YML,
        }


class TestRedshiftCanFindScalarFunctionRelation(CanFindScalarFunctionRelation):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_YML,
        }


class TestRedshiftUDFsWithDatasharing(TestRedshiftUDFs):
    """Same UDF tests with datasharing enabled.

    Exercises metadata queries (get_columns_in_relation) which use SHOW COLUMNS
    in datasharing mode.
    """

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        return {
            "test": {
                "outputs": {
                    "default": {**dbt_profile_target, "schema": unique_schema, "datasharing": True}
                },
                "target": "default",
            }
        }

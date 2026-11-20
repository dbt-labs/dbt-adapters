import pytest

from dbt.tests.adapter.utils import base_utils, fixture_concat


class BaseConcat(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_concat.csv": fixture_concat.seeds__data_concat_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_concat.yml": fixture_concat.models__test_concat_yml,
            "test_concat.sql": self.interpolate_macro_namespace(
                fixture_concat.models__test_concat_sql, "concat"
            ),
        }


class TestConcat(BaseConcat):
    pass


class BaseConcatSingleField(base_utils.BaseUtils):
    """Exercise concat() with a single-element field list.

    Adapters that override default__concat with a SQL CONCAT() function call
    must handle the single-field case specially because CONCAT() requires at
    least two arguments on engines like SQL Server / Fabric T-SQL. The default
    macro short-circuits and returns the lone field unchanged.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_concat.csv": fixture_concat.seeds__data_concat_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_concat_single_field.yml": fixture_concat.models__test_concat_single_field_yml,
            "test_concat_single_field.sql": self.interpolate_macro_namespace(
                fixture_concat.models__test_concat_single_field_sql, "concat"
            ),
        }


class TestConcatSingleField(BaseConcatSingleField):
    pass

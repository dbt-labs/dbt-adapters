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

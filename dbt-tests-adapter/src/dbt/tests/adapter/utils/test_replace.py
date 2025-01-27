import pytest

from dbt.tests.adapter.utils import base_utils, fixture_replace


class BaseReplace(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_replace.csv": fixture_replace.seeds__data_replace_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_replace.yml": fixture_replace.models__test_replace_yml,
            "test_replace.sql": self.interpolate_macro_namespace(
                fixture_replace.models__test_replace_sql, "replace"
            ),
        }


class TestReplace(BaseReplace):
    pass

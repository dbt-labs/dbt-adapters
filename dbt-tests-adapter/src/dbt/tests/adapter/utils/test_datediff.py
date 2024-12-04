import pytest

from dbt.tests.adapter.utils import base_utils, fixture_datediff


class BaseDateDiff(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": fixture_datediff.seeds__data_datediff_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_datediff.yml": fixture_datediff.models__test_datediff_yml,
            "test_datediff.sql": self.interpolate_macro_namespace(
                fixture_datediff.models__test_datediff_sql, "datediff"
            ),
        }


class TestDateDiff(BaseDateDiff):
    pass

import pytest

from dbt.tests.adapter.utils import base_utils, fixture_split_part


class BaseSplitPart(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_split_part.csv": fixture_split_part.seeds__data_split_part_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_split_part.yml": fixture_split_part.models__test_split_part_yml,
            "test_split_part.sql": self.interpolate_macro_namespace(
                fixture_split_part.models__test_split_part_sql, "split_part"
            ),
        }


class TestSplitPart(BaseSplitPart):
    pass

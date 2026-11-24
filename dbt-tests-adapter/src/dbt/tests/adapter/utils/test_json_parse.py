import pytest

from dbt.tests.adapter.utils import base_utils, fixture_json_parse


class BaseJsonParse(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_json_parse.csv": fixture_json_parse.seeds__data_json_parse_csv,
            "data_json_parse_expected.csv": fixture_json_parse.seeds__data_json_parse_expected_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_json_parse.yml": fixture_json_parse.models__test_json_parse_yml,
            "test_json_parse.sql": self.interpolate_macro_namespace(
                fixture_json_parse.models__test_json_parse_sql, "json_parse"
            ),
        }


class TestJsonParse(BaseJsonParse):
    pass

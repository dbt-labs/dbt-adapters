import pytest

from dbt.tests.adapter.utils import base_utils, fixture_cast_bool_to_text


class BaseCastBoolToText(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_cast_bool_to_text.yml": fixture_cast_bool_to_text.models__test_cast_bool_to_text_yml,
            "test_cast_bool_to_text.sql": self.interpolate_macro_namespace(
                fixture_cast_bool_to_text.models__test_cast_bool_to_text_sql, "cast_bool_to_text"
            ),
        }


class TestCastBoolToText(BaseCastBoolToText):
    pass

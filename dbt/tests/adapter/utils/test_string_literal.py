import pytest

from dbt.tests.adapter.utils import base_utils, fixture_string_literal


class BaseStringLiteral(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_string_literal.yml": fixture_string_literal.models__test_string_literal_yml,
            "test_string_literal.sql": self.interpolate_macro_namespace(
                fixture_string_literal.models__test_string_literal_sql, "string_literal"
            ),
        }


class TestStringLiteral(BaseStringLiteral):
    pass

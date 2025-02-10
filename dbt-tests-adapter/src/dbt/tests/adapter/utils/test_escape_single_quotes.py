import pytest

from dbt.tests.adapter.utils import base_utils, fixture_escape_single_quotes


class BaseEscapeSingleQuotesQuote(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_escape_single_quotes.yml": fixture_escape_single_quotes.models__test_escape_single_quotes_yml,
            "test_escape_single_quotes.sql": self.interpolate_macro_namespace(
                fixture_escape_single_quotes.models__test_escape_single_quotes_quote_sql,
                "escape_single_quotes",
            ),
        }


class BaseEscapeSingleQuotesBackslash(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_escape_single_quotes.yml": fixture_escape_single_quotes.models__test_escape_single_quotes_yml,
            "test_escape_single_quotes.sql": self.interpolate_macro_namespace(
                fixture_escape_single_quotes.models__test_escape_single_quotes_backslash_sql,
                "escape_single_quotes",
            ),
        }


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesQuote):
    pass

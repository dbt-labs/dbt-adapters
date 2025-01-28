from typing import Type
from unittest.mock import MagicMock

from dbt_common.exceptions import DbtRuntimeError
import pytest

from dbt.adapters.base.impl import BaseAdapter


class BaseCalculateFreshnessMethod:
    """Tests the behavior of the calculate_freshness_from_customsql method for the relevant adapters.

    The base method is meant to throw the appropriate custom exception when calculate_freshness_from_customsql
    fails.
    """

    @pytest.fixture(scope="class")
    def valid_sql(self) -> str:
        """Returns a valid statement for issuing as a validate_sql query.

        Ideally this would be checkable for non-execution. For example, we could use a
        CREATE TABLE statement with an assertion that no table was created. However,
        for most adapter types this is unnecessary - the EXPLAIN keyword has exactly the
        behavior we want, and here we are essentially testing to make sure it is
        supported. As such, we return a simple SELECT query, and leave it to
        engine-specific test overrides to specify more detailed behavior as appropriate.
        """

        return "select now()"

    @pytest.fixture(scope="class")
    def invalid_sql(self) -> str:
        """Returns an invalid statement for issuing a bad validate_sql query."""

        return "Let's run some invalid SQL and see if we get an error!"

    @pytest.fixture(scope="class")
    def expected_exception(self) -> Type[Exception]:
        """Returns the Exception type thrown by a failed query.

        Defaults to dbt_common.exceptions.DbtRuntimeError because that is the most common
        base exception for adapters to throw."""
        return DbtRuntimeError

    @pytest.fixture(scope="class")
    def mock_relation(self):
        mock = MagicMock()
        mock.__str__ = lambda x: "test.table"
        return mock

    def test_calculate_freshness_from_custom_sql_success(
        self, adapter: BaseAdapter, valid_sql: str, mock_relation
    ) -> None:
        with adapter.connection_named("test_freshness_custom_sql"):
            adapter.calculate_freshness_from_custom_sql(mock_relation, valid_sql)

    def test_calculate_freshness_from_custom_sql_failure(
        self,
        adapter: BaseAdapter,
        invalid_sql: str,
        expected_exception: Type[Exception],
        mock_relation,
    ) -> None:
        with pytest.raises(expected_exception=expected_exception):
            with adapter.connection_named("test_infreshness_custom_sql"):
                adapter.calculate_freshness_from_custom_sql(mock_relation, invalid_sql)


class TestCalculateFreshnessMethod(BaseCalculateFreshnessMethod):
    pass

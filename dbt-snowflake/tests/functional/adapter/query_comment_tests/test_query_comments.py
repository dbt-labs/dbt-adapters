import pytest
from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
    BaseDefaultQueryComments,
)

MODELS__COLUMN_SCHEMA_FROM_QUERY_SQL = """
{% do adapter.get_column_schema_from_query('select 1 as id') %}
select 1 as outer_id
"""


class TestQueryCommentsSnowflake(BaseQueryComments):
    def test_matches_comment(self, project):
        logs = self.run_get_json()
        # No newline in logs because query comment is appended and newline stripped.
        assert r"/* dbt\nrules! */" in logs


class TestMacroQueryCommentsSnowflake(BaseMacroQueryComments):
    def test_matches_comment(self, project):
        logs = self.run_get_json()
        # No newline in logs because query comment is appended and newline stripped.
        assert r"/* dbt macros\nare pretty cool */" in logs


class TestMacroArgsQueryCommentsSnowflake(BaseMacroArgsQueryComments):
    @pytest.mark.skip(
        "This test is incorrectly comparing the version of `dbt-core`"
        "to the version of `dbt-snowflake`, which is not always the same."
    )
    def test_matches_comment(self, project, get_package_version):
        pass


class TestMacroInvalidQueryCommentsSnowflake(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsSnowflake(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsSnowflake(BaseEmptyQueryComments):
    pass


class TestSelectQueryCommentAddedOnceSnowflake(BaseDefaultQueryComments):
    """
    get_column_schema_from_query goes through add_select_query, which used to add the
    query comment on top of the one add_standard_query adds to every statement.
    https://github.com/dbt-labs/dbt-adapters/issues/688
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "x.sql": MODELS__COLUMN_SCHEMA_FROM_QUERY_SQL,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": {"comment": "dbt\nrules!\n", "append": True}}

    def test_comment_added_once(self, project):
        logs = self.run_get_json()
        assert r"select 1 as id\n/* dbt\nrules! */" in logs
        assert r"/* dbt\nrules! */\n/* dbt\nrules! */" not in logs

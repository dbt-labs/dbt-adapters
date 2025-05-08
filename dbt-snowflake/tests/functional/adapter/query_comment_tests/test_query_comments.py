import pytest
from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)


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

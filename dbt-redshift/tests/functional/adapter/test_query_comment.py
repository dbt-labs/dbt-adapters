from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)
import pytest


class TestQueryCommentsRedshift(BaseQueryComments):
    pass


class TestMacroQueryCommentsRedshift(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsRedshift(BaseMacroArgsQueryComments):
    @pytest.mark.skip(
        "This test is incorrectly comparing the version of `dbt-core`"
        "to the version of `dbt-postgres`, which is not always the same."
    )
    def test_matches_comment(self, project, get_package_version):
        pass


class TestMacroInvalidQueryCommentsRedshift(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsRedshift(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsRedshift(BaseEmptyQueryComments):
    pass

import pytest

from dbt_common.exceptions import DbtRuntimeError
from dbt.tests.adapter.dbt_show import fixtures
from dbt.tests.util import run_dbt


# -- Below we define base classes for tests you import based on if your adapter supports dbt show or not --
class BaseShowLimit:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_model.sql": fixtures.models__sample_model,
            "ephemeral_model.sql": fixtures.models__ephemeral_model,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"sample_seed.csv": fixtures.seeds__sample_seed}

    @pytest.mark.parametrize(
        "args,expected",
        [
            ([], 5),  # default limit
            (["--limit", 3], 3),  # fetch 3 rows
            (["--limit", -1], 7),  # fetch all rows
        ],
    )
    def test_limit(self, project, args, expected):
        run_dbt(["build"])
        dbt_args = ["show", "--inline", fixtures.models__second_ephemeral_model, *args]
        results = run_dbt(dbt_args)
        assert len(results.results[0].agate_table) == expected
        # ensure limit was injected in compiled_code when limit specified in command args
        limit = results.args.get("limit")
        if limit > 0:
            assert f"limit {limit}" in results.results[0].node.compiled_code


class BaseShowSqlHeader:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sql_header.sql": fixtures.models__sql_header,
        }

    def test_sql_header(self, project):
        run_dbt(["show", "--select", "sql_header", "--vars", "timezone: Asia/Kolkata"])


class BaseShowDoesNotHandleDoubleLimit:
    """see issue: https://github.com/dbt-labs/dbt-adapters/issues/207"""

    DATABASE_ERROR_MESSAGE = 'syntax error at or near "limit"'

    def test_double_limit_throws_syntax_error(self, project):
        with pytest.raises(DbtRuntimeError) as e:
            run_dbt(["show", "--limit", "1", "--inline", "select 1 limit 1"])

        assert self.DATABASE_ERROR_MESSAGE in str(e)


class TestPostgresShowSqlHeader(BaseShowSqlHeader):
    pass


class TestPostgresShowLimit(BaseShowLimit):
    pass


class TestShowDoesNotHandleDoubleLimit(BaseShowDoesNotHandleDoubleLimit):
    pass

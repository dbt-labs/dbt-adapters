import pytest

from dbt.tests.adapter.basic.test_incremental import (
    BaseIncrementalBadStrategy,
    BaseIncrementalCustomStrategy,
)
from dbt.tests.util import run_dbt


class TestBigQueryCustomIncrementalStrategy(BaseIncrementalCustomStrategy):
    pass


class TestBigQueryBadIncrementalStrategy(BaseIncrementalBadStrategy):
    pass


class TestBigQueryUnsupportedBuiltinIncrementalStrategy:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "append_model.sql": """
{{ config(materialized="incremental", incremental_strategy="append") }}
select 1 as id
"""
        }

    def test_unsupported_builtin_strategy_errors(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "is not valid for this adapter" in results.results[0].message

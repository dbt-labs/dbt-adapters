import os

from dbt.tests.util import run_dbt
import pytest

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange as _BaseOnSchemaChange,
)
from tests.functional.adapter.incremental.test_incremental_on_schema_change import (
    TestIncrementalOnSchemaChangeColumnType as _BaseColumnTypeTest,
    TestIncrementalOnSchemaChangeSpecialChars as _BaseSpecialCharsTest,
)


REDSHIFT_TEST_CROSS_DBNAME = os.getenv("REDSHIFT_TEST_CROSS_DBNAME", "")

_skip_reason = "REDSHIFT_TEST_CROSS_DBNAME not set — skipping cross-database tests"


class _CrossDatabaseMixin:
    """Shared fixtures for cross-database incremental tests."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+database": REDSHIFT_TEST_CROSS_DBNAME,
            }
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["datasharing"] = True
        return outputs


@pytest.mark.skipif(not REDSHIFT_TEST_CROSS_DBNAME, reason=_skip_reason)
class TestIncrementalCrossDatabaseOnSchemaChange(_CrossDatabaseMixin, _BaseOnSchemaChange):
    """Test incremental on_schema_change strategies (ignore, append, sync, fail)
    targeting a cross-database.

    Overrides run_twice_and_assert to skip check_relations_equal which
    cannot resolve columns cross-database.
    """

    def run_twice_and_assert(self, include, compare_source, compare_target, project):
        run_args = ["run"]
        if include:
            run_args.extend(("--select", include))
        results_one = run_dbt(run_args)
        assert len(results_one) == 3

        results_two = run_dbt(run_args)
        assert len(results_two) == 3

    def test_run_incremental_fail_on_schema_change(self, project):
        select = "model_a incremental_fail"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results_two = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Database Error" in results_two[1].message


@pytest.mark.skipif(not REDSHIFT_TEST_CROSS_DBNAME, reason=_skip_reason)
class TestIncrementalCrossDatabaseColumnType(_CrossDatabaseMixin, _BaseColumnTypeTest):
    """Test incremental column type changes (varchar expand, int-to-bigint)
    targeting a cross-database.

    Overrides test methods to skip check_relations_equal which
    cannot resolve columns cross-database.
    """

    def test_incremental_varchar_expand_succeeds_and_matches_target(self, project):
        select = "incremental_varchar_expand incremental_varchar_expand_target"
        run_dbt(["run", "--select", select])
        run_dbt(["run", "--select", select])

    def test_incremental_int_to_bigint_succeeds_and_matches_target(self, project):
        select = "incremental_int_to_bigint incremental_int_to_bigint_target"
        run_dbt(["run", "--select", select])
        run_dbt(["run", "--select", select])


@pytest.mark.skipif(not REDSHIFT_TEST_CROSS_DBNAME, reason=_skip_reason)
class TestIncrementalCrossDatabaseSpecialChars(_CrossDatabaseMixin, _BaseSpecialCharsTest):
    """Test incremental append/sync with special character column names
    targeting a cross-database.

    Overrides test methods to skip check_relations_equal which
    cannot resolve columns cross-database.
    """

    def test_incremental_append_new_columns_with_special_characters(self, project):
        select = "model_a_special_chars incremental_append_new_special_chars incremental_append_new_special_chars_target"
        run_dbt(["run", "--models", select])
        run_dbt(["run", "--models", select])

    def test_incremental_sync_all_columns_with_special_characters(self, project):
        select = "model_a_special_chars incremental_sync_all_special_chars incremental_sync_all_special_chars_target"
        run_dbt(["run", "--models", select])
        run_dbt(["run", "--models", select])

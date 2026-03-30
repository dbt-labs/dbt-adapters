from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.cross_database.conftest import (
    REDSHIFT_TEST_CROSS_DBNAME,
    assert_cross_db_relation_exists,
    skip_if_no_cross_db,
)


_TABLE_IN_DEFAULT_DB = """
{{ config(materialized='table') }}
select 1 as id, 'default_db_data' as label
union all select 2, 'more_data'
"""

_CROSS_DB_TABLE_FROM_DEFAULT = (
    "{{{{ config(materialized='table', database='{cross_db}') }}}}\n"
    "select * from {{{{ ref('table_in_default_db') }}}}"
)

_MODEL_IN_CROSS_DB = (
    "{{{{ config(materialized='table', database='{cross_db}') }}}}\n"
    "select 1 as id, 'cross_db_data' as label\n"
    "union all select 2, 'more_cross_db_data'"
)

_TABLE_REFERENCING_CROSS_DB = """
{{ config(materialized='table') }}
select * from {{ ref('model_in_cross_db') }}
"""

_LATE_BINDING_VIEW_REFERENCING_CROSS_DB = """
{{ config(materialized='view', bind=false) }}
select * from {{ ref('model_in_cross_db') }}
"""


class _CrossRefMixin:
    """Shared profile config for cross-ref tests.

    Only enables datasharing — does NOT set +database at project level,
    since cross-ref tests need models in different databases.
    """

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["datasharing"] = True
        return outputs


@skip_if_no_cross_db
class TestCrossDatabaseForwardRef(_CrossRefMixin):
    """Test a table in the cross-database selecting from the connected database.

    table_in_default_db materializes in the default (connected) database A.
    cross_db_table_from_default materializes in database B and selects from A.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_in_default_db.sql": _TABLE_IN_DEFAULT_DB,
            "cross_db_table_from_default.sql": _CROSS_DB_TABLE_FROM_DEFAULT.format(
                cross_db=REDSHIFT_TEST_CROSS_DBNAME
            ),
        }

    def test_forward_reference(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert_cross_db_relation_exists(
            project.adapter, project.test_schema, "cross_db_table_from_default"
        )


@skip_if_no_cross_db
class TestCrossDatabaseReverseRef(_CrossRefMixin):
    """Test a table in the connected database referencing a table in the cross-database.

    model_in_cross_db materializes in database B.
    table_referencing_cross_db materializes in the default database A and selects from B.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_in_cross_db.sql": _MODEL_IN_CROSS_DB.format(
                cross_db=REDSHIFT_TEST_CROSS_DBNAME
            ),
            "table_referencing_cross_db.sql": _TABLE_REFERENCING_CROSS_DB,
        }

    def test_reverse_reference(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert_cross_db_relation_exists(project.adapter, project.test_schema, "model_in_cross_db")


@skip_if_no_cross_db
class TestCrossDatabaseViewRefFromDefaultDb(_CrossRefMixin):
    """Test a view in the default database referencing a table in the cross-database.

    model_in_cross_db materializes as a table in database B.
    view_referencing_cross_db materializes as a view in the default database A, selecting from B.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_in_cross_db.sql": _MODEL_IN_CROSS_DB.format(
                cross_db=REDSHIFT_TEST_CROSS_DBNAME
            ),
            "view_referencing_cross_db.sql": _LATE_BINDING_VIEW_REFERENCING_CROSS_DB,
        }

    def test_view_referencing_cross_db_table(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert_cross_db_relation_exists(project.adapter, project.test_schema, "model_in_cross_db")

from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.cross_database.fixtures import (
    REDSHIFT_TEST_CROSS_DBNAME,
    CrossDatabaseMixin,
    assert_cross_db_relation_exists,
)


_TABLE_IN_DEFAULT_DB = """
{{ config(materialized='table') }}
select 1 as id, 'default_db_data' as label
union all select 2, 'more_data'
"""

_CROSS_DB_TABLE_FROM_DEFAULT = """
{{ config(materialized='table') }}
select * from {{ ref('table_in_default_db') }}
"""

_MODEL_IN_CROSS_DB = """
{{ config(materialized='table') }}
select 1 as id, 'cross_db_data' as label
union all select 2, 'more_cross_db_data'
"""

_TABLE_REFERENCING_CROSS_DB = """
{{ config(materialized='table') }}
select * from {{ ref('model_in_cross_db') }}
"""

_LATE_BINDING_VIEW_REFERENCING_CROSS_DB = """
{{ config(materialized='view', bind=false) }}
select * from {{ ref('model_in_cross_db') }}
"""


class TestCrossDatabaseForwardRef(CrossDatabaseMixin):
    """Test a table in the cross-database selecting from the connected database.

    table_in_default_db materializes in the default (connected) database A.
    cross_db/cross_db_table_from_default materializes in database B and selects from A.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_in_default_db.sql": _TABLE_IN_DEFAULT_DB,
            "cross_db": {
                "cross_db_table_from_default.sql": _CROSS_DB_TABLE_FROM_DEFAULT,
            },
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "cross_db": {
                        "+database": REDSHIFT_TEST_CROSS_DBNAME,
                    }
                }
            }
        }

    def test_forward_reference(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert_cross_db_relation_exists(
            project.adapter, project.test_schema, "cross_db_table_from_default"
        )


class TestCrossDatabaseReverseRef(CrossDatabaseMixin):
    """Test a table in the connected database referencing a table in the cross-database.

    cross_db/model_in_cross_db materializes in database B.
    table_referencing_cross_db materializes in the default database A and selects from B.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_referencing_cross_db.sql": _TABLE_REFERENCING_CROSS_DB,
            "cross_db": {
                "model_in_cross_db.sql": _MODEL_IN_CROSS_DB,
            },
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "cross_db": {
                        "+database": REDSHIFT_TEST_CROSS_DBNAME,
                    }
                }
            }
        }

    def test_reverse_reference(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert_cross_db_relation_exists(project.adapter, project.test_schema, "model_in_cross_db")


class TestCrossDatabaseViewRefFromDefaultDb(CrossDatabaseMixin):
    """Test a view in the default database referencing a table in the cross-database.

    cross_db/model_in_cross_db materializes as a table in database B.
    view_referencing_cross_db materializes as a view in the default database A, selecting from B.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_referencing_cross_db.sql": _LATE_BINDING_VIEW_REFERENCING_CROSS_DB,
            "cross_db": {
                "model_in_cross_db.sql": _MODEL_IN_CROSS_DB,
            },
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "cross_db": {
                        "+database": REDSHIFT_TEST_CROSS_DBNAME,
                    }
                }
            }
        }

    def test_view_referencing_cross_db_table(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert_cross_db_relation_exists(project.adapter, project.test_schema, "model_in_cross_db")

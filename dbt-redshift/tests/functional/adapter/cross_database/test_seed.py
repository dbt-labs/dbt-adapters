from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.cross_database.conftest import (
    REDSHIFT_TEST_CROSS_DBNAME,
    CrossDatabaseMixin,
    assert_cross_db_relation_exists,
    skip_if_no_cross_db,
)


_SEED_CSV = """id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300
""".lstrip()


@skip_if_no_cross_db
class TestCrossDatabaseSeed(CrossDatabaseMixin):
    """Test seeding data into another database."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+database": REDSHIFT_TEST_CROSS_DBNAME,
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "cross_db_seed.csv": _SEED_CSV,
        }

    def test_seed_into_cross_database(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        assert_cross_db_relation_exists(project.adapter, project.test_schema, "cross_db_seed")

    def test_seed_idempotent(self, project):
        run_dbt(["seed"])
        results = run_dbt(["seed"])
        assert len(results) == 1

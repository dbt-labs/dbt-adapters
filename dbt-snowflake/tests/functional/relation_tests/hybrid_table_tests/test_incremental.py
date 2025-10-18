import pytest

from dbt.tests.util import run_dbt, write_file

from tests.functional.relation_tests.hybrid_table_tests import models
from tests.functional.utils import query_relation_type


class TestHybridTableIncremental:
    """Test incremental behavior of hybrid tables using MERGE"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_hybrid_table_inc.sql": models.HYBRID_TABLE_INCREMENTAL,
        }

    def test_incremental_merge(self, project):
        """Test that subsequent runs use MERGE for updates"""
        # Initial run - creates table with CTAS
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1

        # Verify table was created
        relation_type = query_relation_type(project, "my_hybrid_table_inc")
        assert relation_type == "hybrid_table"

        # Get initial row count
        initial_count = project.run_sql(
            f"select count(*) as cnt from {project.test_schema}.my_hybrid_table_inc", fetch="one"
        )[0]
        assert initial_count == 3

        # Second run - should use MERGE
        results = run_dbt(["run"])
        assert len(results) == 1

        # Verify table still exists and count is same (no new rows in seed)
        relation_type = query_relation_type(project, "my_hybrid_table_inc")
        assert relation_type == "hybrid_table"

        new_count = project.run_sql(
            f"select count(*) as cnt from {project.test_schema}.my_hybrid_table_inc", fetch="one"
        )[0]
        assert new_count == initial_count

    def test_incremental_upsert(self, project):
        """Test that MERGE correctly updates existing rows and inserts new ones"""
        # Initial run
        run_dbt(["seed"])
        run_dbt(["run"])

        # Update seed data with modified values and new row
        write_file(models.SEED_INCREMENTAL_ADD, project.project_root, "seeds", "my_seed.csv")

        # Re-seed with new data
        run_dbt(["seed"])

        # Run again - should MERGE (update existing, insert new)
        run_dbt(["run"])

        # Verify the new row (id=4) was inserted
        inserted = project.run_sql(
            f"select count(*) from {project.test_schema}.my_hybrid_table_inc where id = 4",
            fetch="one",
        )[0]
        assert inserted == 1

        # Verify updated values for existing rows
        updated_value = project.run_sql(
            f"select value from {project.test_schema}.my_hybrid_table_inc where id = 1",
            fetch="one",
        )[0]
        assert updated_value == 150  # Updated from 100 to 150

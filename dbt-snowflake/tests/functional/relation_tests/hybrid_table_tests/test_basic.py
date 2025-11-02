import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

from tests.functional.relation_tests.hybrid_table_tests import models
from tests.functional.utils import query_relation_type


class TestHybridTableBasic:
    """Test basic hybrid table creation and operations"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_hybrid_table.sql": models.HYBRID_TABLE_BASIC,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

    def test_hybrid_table_create(self, project):
        """Test that a hybrid table is created successfully"""
        relation_type = query_relation_type(project, "my_hybrid_table")
        assert relation_type == "hybrid_table"

    def test_hybrid_table_full_refresh(self, project):
        """Test that full refresh works on hybrid tables"""
        run_dbt(["run", "--full-refresh"])
        relation_type = query_relation_type(project, "my_hybrid_table")
        assert relation_type == "hybrid_table"


class TestHybridTableCompositeKey:
    """Test hybrid tables with composite primary keys"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_hybrid_table_composite.sql": models.HYBRID_TABLE_COMPOSITE_KEY,
        }

    def test_composite_primary_key(self, project):
        """Test that hybrid tables with composite keys work"""
        run_dbt(["seed"])
        run_dbt(["run"])

        relation_type = query_relation_type(project, "my_hybrid_table_composite")
        assert relation_type == "hybrid_table"


class TestHybridTableWithIndexes:
    """Test hybrid tables with secondary indexes"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_hybrid_table_indexed.sql": models.HYBRID_TABLE_WITH_INDEX,
            "my_hybrid_table_named_idx.sql": models.HYBRID_TABLE_WITH_NAMED_INDEX,
        }

    def test_hybrid_table_with_indexes(self, project):
        """Test that hybrid tables with indexes are created successfully"""
        run_dbt(["seed"])
        run_dbt(["run"])

        # Verify both tables were created
        assert query_relation_type(project, "my_hybrid_table_indexed") == "hybrid_table"
        assert query_relation_type(project, "my_hybrid_table_named_idx") == "hybrid_table"


class TestHybridTableWithConstraints:
    """Test hybrid tables with various constraints"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_hybrid_table_unique.sql": models.HYBRID_TABLE_WITH_UNIQUE,
        }

    def test_hybrid_table_with_unique_constraint(self, project):
        """Test that hybrid tables with unique constraints work"""
        run_dbt(["seed"])
        run_dbt(["run"])

        relation_type = query_relation_type(project, "my_hybrid_table_unique")
        assert relation_type == "hybrid_table"

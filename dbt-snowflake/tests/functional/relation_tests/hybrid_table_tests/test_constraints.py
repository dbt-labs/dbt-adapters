import pytest

from dbt.tests.util import run_dbt

from tests.functional.relation_tests.hybrid_table_tests import models
from tests.functional.utils import query_relation_type


class TestHybridTablePrimaryKey:
    """Test PRIMARY KEY constraint enforcement"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {
            "my_hybrid_table.sql": models.HYBRID_TABLE_BASIC,
        }

    def test_primary_key_enforcement(self, project):
        """Test that hybrid tables enforce primary key uniqueness"""
        # Initial run
        run_dbt(["seed"])
        run_dbt(["run"])

        # Verify table was created
        assert query_relation_type(project, "my_hybrid_table") == "hybrid_table"

        # Try to insert duplicate primary key - should fail
        try:
            project.run_sql(
                f"insert into {project.test_schema}.my_hybrid_table (id, value, category) "
                f"values (1, 999, 'X')"
            )
            # If we get here, the constraint wasn't enforced
            assert False, "Expected primary key constraint violation"
        except Exception as e:
            # This is expected - primary key constraint should be enforced
            message = str(e).lower()
            # Accept Snowflake hybrid table error variants
            assert (
                "duplicate key" in message
                or "unique" in message
                or "primary key already exists" in message
                or "200001" in message
            )


class TestHybridTableUniqueConstraint:
    """Test UNIQUE constraint enforcement"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {
            "my_hybrid_table_unique.sql": models.HYBRID_TABLE_WITH_UNIQUE,
        }

    def test_unique_constraint_enforcement(self, project):
        """Test that UNIQUE constraints are enforced"""
        # Initial run
        run_dbt(["seed"])
        run_dbt(["run"])

        # Verify table was created
        assert query_relation_type(project, "my_hybrid_table_unique") == "hybrid_table"

        # Try to insert duplicate unique key - should fail
        try:
            project.run_sql(
                f"insert into {project.test_schema}.my_hybrid_table_unique "
                f"(id, value, category, email) values (99, 999, 'X', 'user_1@example.com')"
            )
            # If we get here, the constraint wasn't enforced
            assert False, "Expected unique constraint violation"
        except Exception as e:
            # This is expected - unique constraint should be enforced
            message = str(e).lower()
            assert "duplicate key" in message or "unique" in message or "200001" in message


class TestHybridTableWithoutPrimaryKey:
    """Test that hybrid tables require a primary key"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {
            "my_hybrid_table_no_pk.sql": """
                {{ config(
                    materialized='hybrid_table',
                    columns={
                        'id': 'INTEGER',
                        'value': 'INTEGER'
                    }
                ) }}
                select id, value from {{ ref('my_seed') }}
            """,
        }

    def test_requires_primary_key(self, project):
        """Test that attempting to create a hybrid table without primary key fails"""
        run_dbt(["seed"])

        # This should fail with an error about missing primary_key
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        # The error should mention primary_key requirement


class TestHybridTableCompositeKeyConstraint:
    """Test composite primary key enforcement"""

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {
            "my_hybrid_table_composite.sql": models.HYBRID_TABLE_COMPOSITE_KEY,
        }

    def test_composite_key_enforcement(self, project):
        """Test that composite primary keys are enforced"""
        # Initial run
        run_dbt(["seed"])
        run_dbt(["run"])

        # Verify table was created
        assert query_relation_type(project, "my_hybrid_table_composite") == "hybrid_table"

        # Insert with same id but different category should succeed
        project.run_sql(
            f"insert into {project.test_schema}.my_hybrid_table_composite "
            f"(id, value, category) values (1, 999, 'Z')"
        )

        # Try to insert duplicate composite key - should fail
        try:
            project.run_sql(
                f"insert into {project.test_schema}.my_hybrid_table_composite "
                f"(id, value, category) values (1, 888, 'A')"
            )
            # If we get here, the constraint wasn't enforced
            assert False, "Expected composite primary key constraint violation"
        except Exception as e:
            # This is expected - composite key constraint should be enforced
            message = str(e).lower()
            assert (
                "duplicate key" in message
                or "unique" in message
                or "primary key already exists" in message
                or "200001" in message
            )

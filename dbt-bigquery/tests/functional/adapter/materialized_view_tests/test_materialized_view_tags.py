import pytest

from dbt.adapters.bigquery.relation_configs import BigQueryMaterializedViewConfig
from dbt.tests.util import get_connection, run_dbt
from tests.functional.adapter.materialized_view_tests._mixin import (
    BigQueryMaterializedViewMixin,
)

MY_MATERIALIZED_VIEW_WITH_TAGS = """
{{ config(
    materialized='materialized_view',
    partition_by={
        "field": "record_valid_date",
        "data_type": "datetime",
        "granularity": "day"
    },
    cluster_by=["id", "value"],
    enable_refresh=True,
    refresh_interval_minutes=60,
    bq_tags={"test-project/env": "test", "test-project/team": "data"}
) }}
select
    id,
    value,
    record_valid_date
from {{ ref('my_base_table') }}
"""


MY_SEED = """
id,value,record_valid_date
1,100,2023-01-01 00:00:00
2,200,2023-01-02 00:00:00
3,300,2023-01-02 00:00:00
""".strip()


MY_BASE_TABLE = """
{{ config(
    materialized='table',
    partition_by={
        "field": "record_valid_date",
        "data_type": "datetime",
        "granularity": "day"
    },
    cluster_by=["id", "value"]
) }}
select
    id,
    value,
    record_valid_date
from {{ ref('my_seed') }}
"""


class TestBigQueryMaterializedViewTags(BigQueryMaterializedViewMixin):

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {
            "my_base_table.sql": MY_BASE_TABLE,
            "my_materialized_view_with_tags.sql": MY_MATERIALIZED_VIEW_WITH_TAGS,
        }

    @pytest.fixture(scope="class")
    def my_materialized_view_with_tags(self, project):
        from dbt.adapters.contracts.relation import RelationType

        return project.adapter.Relation.create(
            identifier="my_materialized_view_with_tags",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.MaterializedView,
        )

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_materialized_view_with_bq_tags(self, project, my_materialized_view_with_tags):
        """Test that bq_tags are properly applied to materialized views."""
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(my_materialized_view_with_tags)

        assert isinstance(results, BigQueryMaterializedViewConfig)
        # Check that tags are present in the options
        assert results.options.tags is not None
        expected_tags = {"test-project/env": "test", "test-project/team": "data"}
        assert results.options.tags == expected_tags

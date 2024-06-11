import pytest

from dbt.adapters.capability import Capability
from dbt_common.contracts.metadata import (
    TableMetadata,
    StatsItem,
    CatalogTable,
    ColumnMetadata,
)
from dbt.tests.util import run_dbt, get_connection

models__my_model_sql = """
{{
    config(
        materialized='view',
    )
}}

select * from {{ ref('my_seed') }}
"""

seed__my_seed_csv = """id,first_name,email,ip_address,updated_at
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
"""


class BaseGetCatalogForSingleRelation:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "get_catalog_for_single_relation"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": seed__my_seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": models__my_model_sql,
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass

    def test_get_catalog_for_single_relation(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run"])
        assert len(results) == 1

        if project.adapter.supports(Capability.GetCatalogForSingleRelation):
            expected = CatalogTable(
                metadata=TableMetadata(
                    type="VIEW",
                    schema=project.test_schema.upper(),
                    name="MY_MODEL",
                    database=project.database,
                    comment="",
                    owner="TESTER",
                ),
                columns={
                    "ID": ColumnMetadata(type="NUMBER", index=1, name="ID", comment=None),
                    "FIRST_NAME": ColumnMetadata(
                        type="VARCHAR", index=2, name="FIRST_NAME", comment=None
                    ),
                    "EMAIL": ColumnMetadata(type="VARCHAR", index=3, name="EMAIL", comment=None),
                    "IP_ADDRESS": ColumnMetadata(
                        type="VARCHAR", index=4, name="IP_ADDRESS", comment=None
                    ),
                    "UPDATED_AT": ColumnMetadata(
                        type="TIMESTAMP_NTZ", index=5, name="UPDATED_AT", comment=None
                    ),
                },
                stats={
                    "has_stats": StatsItem(
                        id="has_stats",
                        label="Has Stats?",
                        value=True,
                        include=False,
                        description="Indicates whether there are statistics for this table",
                    ),
                    "row_count": StatsItem(
                        id="row_count",
                        label="Row Count",
                        value=0,
                        include=True,
                        description="Number of rows in the table as reported by Snowflake",
                    ),
                    "bytes": StatsItem(
                        id="bytes",
                        label="Approximate Size",
                        value=0,
                        include=True,
                        description="Size of the table as reported by Snowflake",
                    ),
                },
                unique_id=None,
            )

            with get_connection(project.adapter):
                my_model_relation = project.adapter.get_relation(
                    database=project.database,
                    schema=project.test_schema,
                    identifier="MY_MODEL",
                )
                actual = project.adapter.get_catalog_for_single_relation(my_model_relation)

                assert actual == expected


class TestGetCatalogForSingleRelation(BaseGetCatalogForSingleRelation):
    pass

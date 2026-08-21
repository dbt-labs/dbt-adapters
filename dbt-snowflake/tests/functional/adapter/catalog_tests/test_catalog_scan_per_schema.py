from dbt.contracts.results import CatalogArtifact
from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.catalog_tests import files


class BaseCatalogScanPerSchema:
    """
    Exercises both catalog macros against a database with relations in more than one schema,
    which is the only shape where `snowflake_catalog_scan_per_schema` changes the generated sql.

    The same assertions run with the flag on and off, so the two code paths are held to
    identical results.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_table.sql": files.MY_TABLE,
            "my_view.sql": files.MY_VIEW,
            "my_other_table.sql": files.MY_TABLE_IN_ANOTHER_SCHEMA,
            "my_other_view.sql": files.MY_VIEW_IN_ANOTHER_SCHEMA,
        }

    EXPECTED_TYPES = {
        "seed.test.my_seed": "BASE TABLE",
        "model.test.my_table": "BASE TABLE",
        "model.test.my_view": "VIEW",
        "model.test.my_other_table": "BASE TABLE",
        "model.test.my_other_view": "VIEW",
    }

    @pytest.fixture(scope="class", autouse=True)
    def build(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

    def assert_catalog(self, catalog: CatalogArtifact, node_names):
        assert catalog.errors is None, catalog.errors
        assert set(catalog.nodes) == set(node_names)

        for node_name in node_names:
            node = catalog.nodes[node_name]

            # `table_type` comes out of a case expression over `is_dynamic`, so this also
            # confirms the pruned scans hand every information_schema column to the projection
            assert node.metadata.type == self.EXPECTED_TYPES[node_name]

            # unioning per-schema scans must not drop, duplicate, or reorder columns
            assert [column.name.lower() for column in node.columns.values()] == ["id", "value"]

    def test_get_catalog_spans_every_schema(self, project):
        """
        `snowflake__get_catalog`, filtering by schema.

        `--no-compile` is what selects this macro, not the absence of `--select`. `docs generate`
        compiles by default, which populates the task's job queue, and `GenerateTask` then hands
        `get_filtered_catalog` the full relation set -- so a project this small would take the
        by-relations path even with no selection. Skipping the compile leaves the relation set
        empty and falls back to filtering by schema.
        """
        catalog = run_dbt(["docs", "generate", "--no-compile"])
        self.assert_catalog(catalog, self.EXPECTED_TYPES.keys())

    def test_get_catalog_relations_spans_every_schema(self, project):
        """
        A selection smaller than `MAX_SCHEMA_METADATA_RELATIONS` -> `snowflake__get_catalog_relations`,
        filtering by relation. The selection deliberately straddles both schemas.
        """
        catalog = run_dbt(["docs", "generate", "--select", "my_table", "my_other_view"])
        self.assert_catalog(catalog, ["model.test.my_table", "model.test.my_other_view"])

    def test_get_catalog_relations_for_a_single_schema(self, project):
        """A selection confined to one schema must not pull in the other."""
        catalog = run_dbt(["docs", "generate", "--select", "my_other_table", "my_other_view"])
        self.assert_catalog(catalog, ["model.test.my_other_table", "model.test.my_other_view"])


class TestCatalogScanPerSchemaEnabled(BaseCatalogScanPerSchema):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"snowflake_catalog_scan_per_schema": True}}


class TestCatalogScanPerSchemaDisabled(BaseCatalogScanPerSchema):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"snowflake_catalog_scan_per_schema": False}}

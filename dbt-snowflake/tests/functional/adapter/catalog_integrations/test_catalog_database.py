"""Functional tests for catalog_database precedence in generate_database_name.

Verifies that catalog_database set on a v2 catalog takes highest priority over:
  1. The model's `database` config
  2. target.database

Requires use_catalogs_v2 flag support in dbt-core.
"""

import os
import pytest
from dbt.tests.util import run_dbt, write_config_file

try:
    from dbt.contracts.project import ProjectFlags as _PF

    _has_catalogs_v2 = hasattr(_PF, "use_catalogs_v2")
except ImportError:
    _has_catalogs_v2 = False

pytestmark = pytest.mark.skipif(
    not _has_catalogs_v2,
    reason="dbt-core does not support use_catalogs_v2 yet",
)

ALT_DATABASE = os.getenv("SNOWFLAKE_TEST_ALT_DATABASE")

# catalog_database only — should route to ALT_DATABASE
MODEL__CATALOG_DB_ONLY = """
{{ config(catalog_name='test_catalog') }}
select 'catalog_database only' as scenario
"""

# catalog_database + explicit database config — catalog_database should win
MODEL__CATALOG_DB_BEATS_CONFIG = """
{{ config(catalog_name='test_catalog', database=target.database) }}
select 'catalog_database beats database config' as scenario
"""

# no catalog, no database config — falls back to target.database
MODEL__NO_CATALOG = """
select 'target.database fallback' as scenario
"""


class TestCatalogDatabasePrecedence:
    """catalog_database in v2 catalogs.yml takes highest priority in generate_database_name."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "catalog_db_only.sql": MODEL__CATALOG_DB_ONLY,
            "catalog_db_beats_config.sql": MODEL__CATALOG_DB_BEATS_CONFIG,
            "no_catalog.sql": MODEL__NO_CATALOG,
        }

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_catalog",
                    "type": "horizon",
                    "table_format": "default",
                    "config": {
                        "snowflake": {
                            "catalog_database": ALT_DATABASE,
                        }
                    },
                }
            ]
        }

    @pytest.fixture
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=ALT_DATABASE, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_catalog_database_precedence(self, project, catalogs, clean_up):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        results = run_dbt(["run"])
        assert len(results) == 3

        adapter = project.adapter
        schema = project.test_schema

        # catalog_database routes both models to ALT_DATABASE
        assert (
            adapter.get_relation(ALT_DATABASE, schema, "catalog_db_only") is not None
        ), "catalog_db_only should be in catalog_database (ALT_DATABASE)"
        assert (
            adapter.get_relation(ALT_DATABASE, schema, "catalog_db_beats_config") is not None
        ), "catalog_db_beats_config: catalog_database should win over model database config"

        # no catalog falls back to target.database (not ALT_DATABASE)
        assert (
            adapter.get_relation(project.database, schema, "no_catalog") is not None
        ), "no_catalog should be in target.database"
        assert (
            adapter.get_relation(ALT_DATABASE, schema, "no_catalog") is None
        ), "no_catalog should NOT be in ALT_DATABASE"

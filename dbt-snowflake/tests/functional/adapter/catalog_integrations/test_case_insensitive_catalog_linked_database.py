"""Functional tests for a catalog-linked database with CATALOG_CASE_SENSITIVITY = CASE_INSENSITIVE.

Snowflake creates unquoted names in lowercase in these databases, and SHOW OBJECTS returns them
that way. With default quoting, dbt searches for the uppercased name, so every run after the
first used to fail with ApproximateMatchError (dbt-labs/dbt-adapters#1480).

Requires a Unity-backed catalog-linked database created with
CATALOG_CASE_SENSITIVITY = CASE_INSENSITIVE, and use_catalogs_v2 flag support in dbt-core.
"""

import os
import pytest
from dbt.tests.util import run_dbt, write_config_file

try:
    from dbt.contracts.project import ProjectFlags as _PF

    _has_catalogs_v2 = hasattr(_PF, "use_catalogs_v2")
except ImportError:
    _has_catalogs_v2 = False

CATALOG_LINKED_DATABASE = os.getenv("SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE_UNITY")

pytestmark = [
    pytest.mark.skipif(
        not _has_catalogs_v2,
        reason="dbt-core does not support use_catalogs_v2 yet",
    ),
    pytest.mark.skipif(
        not CATALOG_LINKED_DATABASE,
        reason="SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE_UNITY is not set",
    ),
]

MODEL__UNITY_ICEBERG_TABLE = """
{{ config(materialized='table', catalog_name='unity_case_insensitive') }}
select 1 as id
"""


class TestCaseInsensitiveCatalogLinkedDatabase:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # no quoting config: Snowflake's default leaves every part unquoted
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"unity_iceberg_table.sql": MODEL__UNITY_ICEBERG_TABLE}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "unity_case_insensitive",
                    "type": "unity",
                    "table_format": "iceberg",
                    "config": {
                        "snowflake": {
                            "catalog_database": CATALOG_LINKED_DATABASE,
                        }
                    },
                }
            ]
        }

    @pytest.fixture
    def clean_up(self, project):
        yield
        # drop_relation and drop_schema add CASCADE, which catalog-linked databases reject (004529)
        schema = f"{CATALOG_LINKED_DATABASE}.{project.test_schema}"
        project.run_sql(f"drop table if exists {schema}.unity_iceberg_table")
        project.run_sql(f"drop schema if exists {schema}")

    def test_rerun_replaces_existing_table(self, project, catalogs, clean_up):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        run_dbt(["run"])
        # the second run looks up the table the first run created
        run_dbt(["run"])

        with project.adapter.connection_named("__test"):
            relation = project.adapter.get_relation(
                CATALOG_LINKED_DATABASE, project.test_schema, "unity_iceberg_table"
            )
        assert relation is not None
        # stored in lowercase on the remote catalog
        assert relation.identifier == "unity_iceberg_table"

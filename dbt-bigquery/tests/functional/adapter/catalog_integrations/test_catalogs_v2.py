"""Functional tests for catalogs.yml v2 on BigQuery.

Requires use_catalogs_v2 flag support in dbt-core (PR #12930).
"""

import os
import pytest
from dbt.tests.util import run_dbt, write_config_file

# Skip if installed dbt-core doesn't support use_catalogs_v2 yet (requires PR #12930).
try:
    from dbt.contracts.project import ProjectFlags as _PF

    _has_catalogs_v2 = hasattr(_PF, "use_catalogs_v2")
except ImportError:
    _has_catalogs_v2 = False

pytestmark = pytest.mark.skipif(
    not _has_catalogs_v2,
    reason="dbt-core does not support use_catalogs_v2 yet (requires PR #12930)",
)

_BQ_BUCKET = f"gs://{os.getenv('BIGQUERY_TEST_ICEBERG_BUCKET')}"

MODEL__BIGLAKE_ICEBERG = """
{{ config(materialized='table', catalog='bq_biglake_v2') }}
select 1 as id
"""


class TestBigQueryV2BigLakeCatalog:
    """End-to-end test: v2 biglake_metastore catalog → bridge → BigLakeCatalogIntegration → DDL."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models": {
                "biglake_iceberg.sql": MODEL__BIGLAKE_ICEBERG,
            }
        }

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "bq_biglake_v2",
                    "type": "biglake_metastore",
                    "table_format": "iceberg",
                    "config": {
                        "bigquery": {
                            "external_volume": _BQ_BUCKET,
                            "file_format": "parquet",
                        }
                    },
                }
            ]
        }

    def test_biglake_v2_runs(self, project, catalogs):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        run_dbt(["run"])

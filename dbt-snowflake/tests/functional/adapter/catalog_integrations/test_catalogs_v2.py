"""Functional tests for catalogs.yml v2 on Snowflake.

Requires use_catalogs_v2 flag support in dbt-core (PR #12930).
"""

import re
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


def get_cleaned_model_ddl_from_file(file_name: str) -> str:
    with open(f"target/run/test/models/{file_name}", "r") as ddl_file:
        return re.sub(r"\s+", " ", ddl_file.read())


MODEL__HORIZON_ICEBERG = """
{{ config(materialized='table', catalog_name='sf_horizon_v2') }}
select 1 as id
"""

MODEL__HORIZON_WITH_CONFIGS = """
{{ config(
    materialized='table',
    catalog_name='sf_horizon_v2',
    data_retention_time_in_days=1,
    change_tracking=False,
    max_data_extension_time_in_days=30,
    storage_serialization_policy='COMPATIBLE'
) }}
select 1 as id
"""


class TestSnowflakeV2HorizonCatalog:
    """End-to-end test: v2 horizon catalog → bridge → BuiltInCatalogIntegration → DDL."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "horizon_iceberg.sql": MODEL__HORIZON_ICEBERG,
            "horizon_with_configs.sql": MODEL__HORIZON_WITH_CONFIGS,
        }

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "sf_horizon_v2",
                    "type": "horizon",
                    "table_format": "iceberg",
                    "config": {
                        "snowflake": {
                            "external_volume": "s3_iceberg_snow",
                            "storage_serialization_policy": "OPTIMIZED",
                            "max_data_extension_time_in_days": 60,
                            "data_retention_time_in_days": 0,
                            "change_tracking": True,
                        }
                    },
                }
            ]
        }

    def test_horizon_v2_generates_correct_ddl(self, project, catalogs):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        run_dbt(["run"])

        basic_sql = get_cleaned_model_ddl_from_file("horizon_iceberg.sql")
        assert "external_volume = 's3_iceberg_snow'" in basic_sql
        assert "storage_serialization_policy = 'OPTIMIZED'" in basic_sql
        assert "change_tracking = TRUE" in basic_sql

        configs_sql = get_cleaned_model_ddl_from_file("horizon_with_configs.sql")
        assert "storage_serialization_policy = 'COMPATIBLE'" in configs_sql
        assert "change_tracking = FALSE" in configs_sql
        assert "max_data_extension_time_in_days = 30" in configs_sql
        assert "data_retention_time_in_days = 1" in configs_sql

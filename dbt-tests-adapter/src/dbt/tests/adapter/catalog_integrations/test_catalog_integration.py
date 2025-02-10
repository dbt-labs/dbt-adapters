import pytest
from typing import Dict
from dbt.tests.util import run_dbt, write_config_file

CATALOG_NAME = "test_catalog"
BASIC_CATALOG_INTEGRATION_TABLE_MODEL = """
{{
  config(
    materialized = "table",
    catalog_name = "test_catalog",
  )
}}
select 1 as id
"""


class BaseCatalogIntegration:
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {
            "basic_table_model.sql": BASIC_CATALOG_INTEGRATION_TABLE_MODEL,
        }

    @pytest.fixture(scope="class")
    def write_catalog_integration(self) -> Dict:
        return {}

    @pytest.fixture(scope="class", autouse=True)
    def catalogs(self, write_catalog_integration, project):
        catalogs = {
            "catalogs": [
                {
                    "name": CATALOG_NAME,
                    "write_integrations": [
                        write_catalog_integration
                    ],
                }
            ]
        }
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        return catalogs

    def test_catalog_integration(self, project, catalogs):
        run_dbt(["run"])

import pytest
from dbt.tests.util import write_config_file


class BaseCatalogIntegrationValidation:
    @pytest.fixture
    def catalogs(self):
        return {}

    @pytest.fixture(scope="class", autouse=True)
    def write_catalogs_config_file(self, project, catalogs):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

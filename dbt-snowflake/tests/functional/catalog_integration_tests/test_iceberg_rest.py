import yaml

import pytest

from tests.functional.catalog_integration_tests import _files


class IcebergREST:

    @pytest.mark.skip(reason="infra is not ready yet")
    def test_table_gets_created(self, project):
        results = project.run_dbt(["run"])
        assert len(results) == 1
        records = project.run_sql("select * from my_model", fetch="all")
        assert len(records) > 0


class TestIcebergREST(IcebergREST):

    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        catalogs = yaml.load(_files.ICEBERG_REST_CATALOG, Loader=yaml.SafeLoader)
        secondary_profile = yaml.load(_files.SECONDARY_PROFILE, Loader=yaml.SafeLoader)
        return {**catalogs, **secondary_profile}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": _files.ICEBERG_REST_MODEL}


class TestAWSGlue(IcebergREST):

    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        catalogs = yaml.load(_files.ICEBERG_REST_CATALOG, Loader=yaml.SafeLoader)
        secondary_profile = yaml.load(_files.SECONDARY_PROFILE, Loader=yaml.SafeLoader)
        return {**catalogs, **secondary_profile}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": _files.AWS_GLUE_MODEL}

import pytest

from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class TestModelGrantsRedshift(BaseModelGrants):
    pass


class TestIncrementalGrantsRedshift(BaseIncrementalGrants):
    pass


class TestSeedGrantsRedshift(BaseSeedGrants):
    pass


class TestSnapshotGrantsRedshift(BaseSnapshotGrants):
    pass


class TestInvalidGrantsRedshift(BaseModelGrants):
    pass


class TestModelGrantsRedshiftWithShowApis(BaseModelGrants):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"redshift_use_show_apis": True}}


class TestIncrementalGrantsRedshiftWithShowApis(BaseIncrementalGrants):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"redshift_use_show_apis": True}}


class TestSeedGrantsRedshiftWithShowApis(BaseSeedGrants):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"redshift_use_show_apis": True}}


class TestSnapshotGrantsRedshiftWithShowApis(BaseSnapshotGrants):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"redshift_use_show_apis": True}}

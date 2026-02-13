import pytest

from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class RedshiftGrantsMixin:
    """Normalize expected grants to use 'user:' prefix for consistent comparison.

    Redshift's get_show_grant_sql returns grantee names with 'user:' or 'group:'
    prefixes. The base test expected dicts use plain usernames, so we normalize
    them before comparison.
    """

    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        normalized = {}
        for privilege, grantees in expected_grants.items():
            normalized[privilege] = [
                g if g.startswith(("user:", "group:", "role:")) else f"user:{g}" for g in grantees
            ]
        super().assert_expected_grants_match_actual(project, relation_name, normalized)


class TestModelGrantsRedshift(RedshiftGrantsMixin, BaseModelGrants):
    pass


class TestIncrementalGrantsRedshift(RedshiftGrantsMixin, BaseIncrementalGrants):
    pass


class TestSeedGrantsRedshift(RedshiftGrantsMixin, BaseSeedGrants):
    pass


class TestSnapshotGrantsRedshift(RedshiftGrantsMixin, BaseSnapshotGrants):
    pass


class TestInvalidGrantsRedshift(RedshiftGrantsMixin, BaseModelGrants):
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

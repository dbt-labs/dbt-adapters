import pytest

from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class RedshiftGrantsMixin:
    """Normalize expected grants to use 'user:' prefix for consistent comparison.

    Redshift's get_show_grant_sql returns grantee names with 'user:', 'group:',
    or 'role:' prefixes when redshift_grants_extended is enabled. The base test
    expected dicts use plain usernames, so we normalize them before comparison.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"redshift_grants_extended": True}}

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


class TestModelGrantsRedshiftWithDatasharing(RedshiftGrantsMixin, BaseModelGrants):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        return {
            "test": {
                "outputs": {
                    "default": {**dbt_profile_target, "schema": unique_schema, "datasharing": True}
                },
                "target": "default",
            }
        }


class TestIncrementalGrantsRedshiftWithDatasharing(RedshiftGrantsMixin, BaseIncrementalGrants):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        return {
            "test": {
                "outputs": {
                    "default": {**dbt_profile_target, "schema": unique_schema, "datasharing": True}
                },
                "target": "default",
            }
        }


class TestSeedGrantsRedshiftWithDatasharing(RedshiftGrantsMixin, BaseSeedGrants):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        return {
            "test": {
                "outputs": {
                    "default": {**dbt_profile_target, "schema": unique_schema, "datasharing": True}
                },
                "target": "default",
            }
        }


class TestSnapshotGrantsRedshiftWithDatasharing(RedshiftGrantsMixin, BaseSnapshotGrants):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        return {
            "test": {
                "outputs": {
                    "default": {**dbt_profile_target, "schema": unique_schema, "datasharing": True}
                },
                "target": "default",
            }
        }


class TestModelGrantsRedshiftLegacy(BaseModelGrants):
    """Legacy grants (redshift_grants_extended=False, default).

    Uses the pg_user + has_table_privilege() path.  The adapter returns plain
    usernames so no normalization is needed — base test assertions work as-is.
    Only user grants are visible; groups and roles are unsupported in this path.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"redshift_grants_extended": False}}


class TestIncrementalGrantsRedshiftLegacy(BaseIncrementalGrants):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"redshift_grants_extended": False}}


class TestSeedGrantsRedshiftLegacy(BaseSeedGrants):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"redshift_grants_extended": False}}


class TestSnapshotGrantsRedshiftLegacy(BaseSnapshotGrants):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"redshift_grants_extended": False}}

import pytest
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey


class TestUniqueKeyRedshift(BaseIncrementalUniqueKey):
    pass


class TestUniqueKeyDeleteInsertRedshift(BaseIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "delete+insert"}}


class TestUniqueKeyRedshiftWithDatasharing(TestUniqueKeyRedshift):
    """Same incremental unique key tests with datasharing enabled.

    Exercises list_relations_without_caching which uses SHOW TABLES in datasharing mode.
    """

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["datasharing"] = True
        return outputs

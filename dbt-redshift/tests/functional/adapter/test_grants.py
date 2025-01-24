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

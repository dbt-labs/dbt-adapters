from dbt.tests.adapter import grants


class TestIncrementalGrants(grants.BaseIncrementalGrants):
    pass


class TestInvalidGrants(grants.BaseInvalidGrants):
    pass


class TestModelGrants(grants.BaseModelGrants):
    pass


class TestSeedGrants(grants.BaseSeedGrants):
    pass


class TestSnapshotGrants(grants.BaseSnapshotGrants):
    pass

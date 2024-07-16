from dbt.tests.adapter.ephemeral.test_ephemeral import (
    BaseEphemeralMulti,
    BaseEphemeralNested,
    BaseEphemeralErrorHandling,
)


class TestEphemeralMulti(BaseEphemeralMulti):
    pass


class TestEphemeralNested(BaseEphemeralNested):
    pass


class TestEphemeralErrorHandling(BaseEphemeralErrorHandling):
    pass

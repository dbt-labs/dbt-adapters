from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
    BaseTestEmptySeedFlag,
)


class TestAthenaEmpty(BaseTestEmpty):
    pass


class TestAthenaEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


class TestAthenaEmptySeedFlag(BaseTestEmptySeedFlag):
    pass

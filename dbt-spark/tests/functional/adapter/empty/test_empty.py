from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
    BaseTestEmptySeedFlag,
)


class TestSparkEmpty(BaseTestEmpty):
    pass


class TestSparkEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


class TestSparkEmptySeedFlag(BaseTestEmptySeedFlag):
    pass

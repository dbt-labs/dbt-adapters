from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
    BaseTestEmptySeedFlag,
)


class TestBigQueryEmpty(BaseTestEmpty):
    pass


class TestBigQueryEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


class TestBigQueryEmptySeedFlag(BaseTestEmptySeedFlag):
    pass

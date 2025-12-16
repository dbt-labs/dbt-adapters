from dbt.tests.adapter.empty.test_empty import BaseTestEmpty, BaseTestEmptyInlineSourceRef, BaseTestEmptySeed


class TestBigQueryEmpty(BaseTestEmpty):
    pass


class TestBigQueryEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


class TestBigQueryEmptySeed(BaseTestEmptySeed):
    pass
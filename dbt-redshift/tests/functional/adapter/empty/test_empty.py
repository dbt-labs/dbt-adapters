from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
    BaseTestEmptySeedFlag,
)


class TestRedshiftEmpty(BaseTestEmpty):
    pass


class TestRedshiftEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


class TestRedshiftEmptySeedFlag(BaseTestEmptySeedFlag):
    pass

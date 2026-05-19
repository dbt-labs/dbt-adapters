from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
    BaseTestEmptySeedFlag,
    MetadataWithEmptyFlag,
)


class TestSnowflakeEmpty(BaseTestEmpty):
    pass


class TestSnowflakeEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


class TestSnowflakeEmptySeedFlag(BaseTestEmptySeedFlag):
    pass


class TestMetadataWithEmptyFlag(MetadataWithEmptyFlag):
    pass

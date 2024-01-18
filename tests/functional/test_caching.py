from dbt.tests.adapter.caching.test_caching import (
    BaseCachingNoPopulateCache,
    BaseCachingLowercaseModel,
    BaseCachingUppercaseModel,
    BaseCachingSelectedSchemaOnly,
)


class TestCachingNoPopulateCache(BaseCachingNoPopulateCache):
    pass


class TestCachingLowerCaseModel(BaseCachingLowercaseModel):
    pass


class TestCachingUppercaseModel(BaseCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnly(BaseCachingSelectedSchemaOnly):
    pass

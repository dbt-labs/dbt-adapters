"""
This file needs to be in its own directory because it creates a `data` directory at run time.
Placing this file in its own directory avoids collisions.
"""
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseBasicSeedTests,
    BaseSeedConfigFullRefreshOn,
    BaseSeedConfigFullRefreshOff,
    BaseSeedCustomSchema,
    BaseSeedWithUniqueDelimiter,
    BaseSeedWithWrongDelimiter,
    BaseSeedWithEmptyDelimiter,
    BaseSimpleSeedEnabledViaConfig,
    BaseSeedParsing,
    BaseSimpleSeedWithBOM,
    BaseSeedSpecificFormats,
    BaseTestEmptySeed,
)
from dbt.tests.adapter.simple_seed.test_seed_type_override import (
    BaseSimpleSeedColumnOverride,
)


class TestBasicSeedTests(BaseBasicSeedTests):
    pass


class TestSeedConfigFullRefreshOn(BaseSeedConfigFullRefreshOn):
    pass


class TestSeedConfigFullRefreshOff(BaseSeedConfigFullRefreshOff):
    pass


class TestSeedCustomSchema(BaseSeedCustomSchema):
    pass


class TestSeedWithUniqueDelimiter(BaseSeedWithUniqueDelimiter):
    pass


class TestSeedWithWrongDelimiter(BaseSeedWithWrongDelimiter):
    pass


class TestSeedWithEmptyDelimiter(BaseSeedWithEmptyDelimiter):
    pass


class TestSimpleSeedEnabledViaConfig(BaseSimpleSeedEnabledViaConfig):
    pass


class TestSeedParsing(BaseSeedParsing):
    pass


class TestSimpleSeedWithBOM(BaseSimpleSeedWithBOM):
    pass


class TestSeedSpecificFormats(BaseSeedSpecificFormats):
    pass


class TestEmptySeed(BaseTestEmptySeed):
    pass


class TestSimpleSeedColumnOverride(BaseSimpleSeedColumnOverride):
    pass

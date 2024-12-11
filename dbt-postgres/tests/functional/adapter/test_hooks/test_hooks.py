"""
This file needs to be in its own directory because it uses a `data` directory.
Placing this file in its own directory avoids collisions.
"""

from dbt.tests.adapter.hooks.test_model_hooks import (
    BasePrePostModelHooks,
    BaseHookRefs,
    BasePrePostModelHooksOnSeeds,
    BaseHooksRefsOnSeeds,
    BasePrePostModelHooksOnSeedsPlusPrefixed,
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace,
    BasePrePostModelHooksOnSnapshots,
    BasePrePostModelHooksInConfig,
    BasePrePostModelHooksInConfigWithCount,
    BasePrePostModelHooksInConfigKwargs,
    BasePrePostSnapshotHooksInConfigKwargs,
    BaseDuplicateHooksInConfigs,
)
from dbt.tests.adapter.hooks.test_run_hooks import (
    BasePrePostRunHooks,
    BaseAfterRunHooks,
)


class TestPrePostModelHooks(BasePrePostModelHooks):
    pass


class TestHookRefs(BaseHookRefs):
    pass


class TestPrePostModelHooksOnSeeds(BasePrePostModelHooksOnSeeds):
    pass


class TestHooksRefsOnSeeds(BaseHooksRefsOnSeeds):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixed(BasePrePostModelHooksOnSeedsPlusPrefixed):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace(
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace
):
    pass


class TestPrePostModelHooksOnSnapshots(BasePrePostModelHooksOnSnapshots):
    pass


class TestPrePostModelHooksInConfig(BasePrePostModelHooksInConfig):
    pass


class TestPrePostModelHooksInConfigWithCount(BasePrePostModelHooksInConfigWithCount):
    pass


class TestPrePostModelHooksInConfigKwargs(BasePrePostModelHooksInConfigKwargs):
    pass


class TestPrePostSnapshotHooksInConfigKwargs(BasePrePostSnapshotHooksInConfigKwargs):
    pass


class TestDuplicateHooksInConfigs(BaseDuplicateHooksInConfigs):
    pass


class TestPrePostRunHooks(BasePrePostRunHooks):
    pass


class TestAfterRunHooks(BaseAfterRunHooks):
    pass

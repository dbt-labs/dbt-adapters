from dbt.tests.adapter import hooks


class TestBaseAfterRunHooks(hooks.BaseAfterRunHooks):
    pass


class TestBasePrePostRunHooks(hooks.BasePrePostRunHooks):
    pass


class TestDuplicateHooksInConfigs(hooks.DuplicateHooksInConfigs):
    pass


class TestHookRefs(hooks.HookRefs):
    pass


class TestHooksRefsOnSeeds(hooks.HooksRefsOnSeeds):
    pass


class TestPrePostModelHooks(hooks.PrePostModelHooks):
    pass


class TestPrePostModelHooksInConfig(hooks.PrePostModelHooksInConfig):
    pass


class TestPrePostModelHooksInConfigKwargs(hooks.PrePostModelHooksInConfigKwargs):
    pass


class TestPrePostModelHooksInConfigWithCount(hooks.PrePostModelHooksInConfigWithCount):
    pass


class TestPrePostModelHooksOnSeeds(hooks.PrePostModelHooksOnSeeds):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixed(hooks.PrePostModelHooksOnSeedsPlusPrefixed):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace(hooks.PrePostModelHooksOnSeedsPlusPrefixedWhitespace):
    pass


class TestPrePostModelHooksOnSnapshots(hooks.PrePostModelHooksOnSnapshots):
    pass


class TestPrePostModelHooksUnderscores(hooks.PrePostModelHooksUnderscores):
    pass


class TestPrePostSnapshotHooksInConfigKwargs(hooks.PrePostSnapshotHooksInConfigKwargs):
    pass

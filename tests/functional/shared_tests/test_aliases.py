from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliases,
    BaseAliasErrors,
    BaseSameAliasDifferentSchemas,
    BaseSameAliasDifferentDatabases,
)


class TestAliases(BaseAliases):
    pass


class TestAliasErrors(BaseAliasErrors):
    pass


class TestSameAliasDifferentSchemas(BaseSameAliasDifferentSchemas):
    pass


class TestSameAliasDifferentDatabases(BaseSameAliasDifferentDatabases):
    pass

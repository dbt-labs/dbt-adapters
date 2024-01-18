from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliasErrors,
    BaseAliases,
    BaseSameAliasDifferentDatabases,
    BaseSameAliasDifferentSchemas,
)


class TestAliases(BaseAliases):
    pass


class TestAliasErrors(BaseAliasErrors):
    pass


class TestSameAliasDifferentSchemas(BaseSameAliasDifferentSchemas):
    pass


class TestSameAliasDifferentDatabases(BaseSameAliasDifferentDatabases):
    pass

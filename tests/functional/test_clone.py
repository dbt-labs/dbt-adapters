from dbt.tests.adapter.dbt_clone.test_dbt_clone import (
    BaseCloneNotPossible,
    BaseCloneSameTargetAndState,
)


class TestPostgresCloneNotPossible(BaseCloneNotPossible):
    pass


class TestPostgresCloneSameTargetAndState(BaseCloneSameTargetAndState):
    pass

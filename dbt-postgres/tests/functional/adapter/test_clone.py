import pytest

from dbt.tests.adapter.dbt_clone.test_dbt_clone import (
    BaseCloneNotPossible,
    BaseClonePossible,
    BaseCloneSameSourceAndTargetNotPossible,
    BaseCloneSameTargetAndState,
)


class TestBaseCloneNotPossible(BaseCloneNotPossible):
    pass


class TestBaseCloneSameSourceAndTargetNotPossible(BaseCloneSameSourceAndTargetNotPossible):
    pass


@pytest.mark.skip("Cloning is not possible in Postgres")
class TestBaseClonePossible(BaseClonePossible):
    pass


class TestCloneSameTargetAndState(BaseCloneSameTargetAndState):
    pass

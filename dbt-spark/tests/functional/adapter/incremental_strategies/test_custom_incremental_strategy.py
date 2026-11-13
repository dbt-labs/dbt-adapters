import pytest

from dbt.tests.adapter.basic.test_incremental import (
    BaseIncrementalBadStrategy,
    BaseIncrementalCustomStrategy,
)


@pytest.mark.skip_profile("spark_session")
class TestSparkCustomIncrementalStrategy(BaseIncrementalCustomStrategy):
    pass


@pytest.mark.skip_profile("spark_session")
class TestSparkBadIncrementalStrategy(BaseIncrementalBadStrategy):
    pass

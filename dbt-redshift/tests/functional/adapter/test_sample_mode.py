import pytest
from dbt.tests.adapter.sample_mode.test_sample_mode import (
    BaseSampleModeTest,
)


@pytest.mark.skip(reason="Sample mode is unsupported on 1.9")
class TestRedshiftSampleMode(BaseSampleModeTest):
    pass

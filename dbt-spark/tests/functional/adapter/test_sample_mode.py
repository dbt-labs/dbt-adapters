from dbt.tests.adapter.sample_mode.test_sample_mode import (
    BaseSampleModeTest,
)
import pytest


@pytest.mark.skip_profile(
    "databricks_http_cluster", "databricks_sql_endpoint", "spark_session", "spark_http_odbc"
)
class TestSparkSampleMode(BaseSampleModeTest):
    pass

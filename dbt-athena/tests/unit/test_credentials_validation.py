import pytest

from dbt.adapters.athena.connections import AthenaCredentials
from dbt_common.exceptions import DbtRuntimeError

from tests.unit import constants


def _make(**overrides):
    base = dict(
        database=constants.DATA_CATALOG_NAME,
        schema=constants.DATABASE_NAME,
        s3_staging_dir=constants.S3_STAGING_DIR,
        region_name=constants.AWS_REGION,
        work_group=constants.ATHENA_WORKGROUP,
        spark_work_group=constants.SPARK_WORKGROUP,
    )
    base.update(overrides)
    return AthenaCredentials(**base)


class TestSparkConnectIntegerValidation:
    """Profile-load validation guards against typos in Spark Connect int fields."""

    def test_max_retries_zero_is_accepted(self):
        # 0 retries = single attempt; allowed semantic since rebuild 15 fix.
        c = _make(spark_connect_max_retries=0)
        assert c.spark_connect_max_retries == 0

    def test_max_retries_positive_is_accepted(self):
        c = _make(spark_connect_max_retries=3)
        assert c.spark_connect_max_retries == 3

    def test_max_retries_negative_is_rejected(self):
        with pytest.raises(
            DbtRuntimeError, match="spark_connect_max_retries must be a non-negative integer"
        ):
            _make(spark_connect_max_retries=-1)

    @pytest.mark.parametrize(
        "field_name",
        [
            "spark_connect_max_sessions",
            "spark_connect_session_concurrency",
            "spark_connect_dpu_budget",
            "spark_connect_pool_acquire_timeout",
        ],
    )
    def test_count_fields_reject_zero(self, field_name):
        # Counts / sizes / timeouts have no meaning at 0 — keep the strict guard.
        with pytest.raises(DbtRuntimeError, match=f"{field_name} must be a positive integer"):
            _make(**{field_name: 0})

    @pytest.mark.parametrize(
        "field_name",
        [
            "spark_connect_max_sessions",
            "spark_connect_session_concurrency",
            "spark_connect_dpu_budget",
            "spark_connect_pool_acquire_timeout",
        ],
    )
    def test_count_fields_accept_one(self, field_name):
        c = _make(**{field_name: 1})
        assert getattr(c, field_name) == 1

    def test_none_is_passed_through(self):
        # Explicitly omitted fields stay None so the runtime falls back to defaults.
        c = _make()
        assert c.spark_connect_max_retries is None
        assert c.spark_connect_dpu_budget is None


class TestSparkConnectKeepaliveIntervalValidation:
    def test_zero_disables(self):
        assert _make(spark_connect_keepalive_interval=0).spark_connect_keepalive_interval == 0

    def test_positive_is_accepted(self):
        assert _make(spark_connect_keepalive_interval=120).spark_connect_keepalive_interval == 120

    def test_negative_is_rejected(self):
        with pytest.raises(
            DbtRuntimeError,
            match="spark_connect_keepalive_interval must be a non-negative integer",
        ):
            _make(spark_connect_keepalive_interval=-1)

    @pytest.mark.parametrize("value", [600, 900])
    def test_interval_not_shorter_than_idle_timeout_is_rejected(self, value):
        with pytest.raises(DbtRuntimeError, match="shorter than the Spark session idle timeout"):
            _make(spark_connect_keepalive_interval=value)

    def test_interval_just_below_idle_timeout_is_accepted(self):
        assert _make(spark_connect_keepalive_interval=599).spark_connect_keepalive_interval == 599

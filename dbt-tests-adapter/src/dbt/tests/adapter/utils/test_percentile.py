import pytest

from dbt.tests.adapter.utils import base_utils, fixture_percentile


class BasePercentile(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_percentile.csv": fixture_percentile.seeds__data_percentile_csv,
            "data_percentile_expected.csv": fixture_percentile.seeds__data_percentile_expected_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_percentile.yml": fixture_percentile.models__test_percentile_yml,
            "test_percentile.sql": self.interpolate_macro_namespace(
                fixture_percentile.models__test_percentile_sql, "percentile"
            ),
        }


class BasePercentileAggregate(BasePercentile):
    """For adapters where percentile is an aggregate, such as Postgres."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_percentile.yml": fixture_percentile.models__test_percentile_yml,
            "test_percentile.sql": self.interpolate_macro_namespace(
                fixture_percentile.models__test_percentile_aggregate_sql, "percentile"
            ),
        }


class TestPercentile(BasePercentile):
    pass

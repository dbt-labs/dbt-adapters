import pytest
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps


class TestCurrentTimestampRedshift(BaseCurrentTimestamps):
    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "current_timestamp": "timestamp without time zone",
            "current_timestamp_in_utc_backcompat": "timestamp without time zone",
            "current_timestamp_backcompat": "timestamp without time zone",
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
                select getdate() as current_timestamp,
                       getdate() as current_timestamp_in_utc_backcompat,
                       getdate() as current_timestamp_backcompat
                """
